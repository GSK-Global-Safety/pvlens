package org.pvlens.spl.processing.persist;

/*
 * This file is part of PVLens.
 *
 * Copyright (C) 2025 GlaxoSmithKline
 *
 * PVLens is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * PVLens is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with PVLens.  If not, see <https://www.gnu.org/licenses/>.
 */

import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang3.StringUtils;
import org.pvlens.spl.conf.ConfigLoader;
import org.pvlens.spl.om.Outcome;
import org.pvlens.spl.om.SplDrug;
import org.pvlens.spl.umls.Atom;
import org.pvlens.spl.umls.UmlsLoader;
import org.pvlens.spl.util.Logger;

/**
 * Emits SQL artifacts for the merged {@link SplDrug} products and their related
 * entities (SRLC, SPL source files, NDC/ATC, RxNorm, SNOMED, IND/AEs).
 *
 * <p>Behavior matches the existing implementation; changes are limited to
 * comment formalization, very small clarity fixes, and a corrected log line.</p>
 */
public class ProductPersister {

	private final UmlsLoader umls;
	private final ConfigLoader cfg;

	// Thread-safe (java.time)
	private static final DateTimeFormatter DB_FMT =
			DateTimeFormatter.ofPattern(org.pvlens.spl.processing.support.Dates.DB_FMT, Locale.ROOT);

	/**
	 * Formats a java.util.Date with the DB pattern. Works for both date-only
	 * ("yyyy-MM-dd") and date-time ("yyyy-MM-dd HH:mm:ss") patterns.
	 */
	private static String fmt(Date d) {
		if (d == null) return null;
		ZonedDateTime zdt = d.toInstant().atZone(ZoneOffset.UTC);
		return DB_FMT.format(zdt);
	}

	public ProductPersister(UmlsLoader umls) {
		this.umls = Objects.requireNonNull(umls);
		this.cfg = new ConfigLoader();
	}

	/**
	 * Persist the full product set to SQL writers. Runs sequentially to avoid
	 * interleaving output across files.
	 */
	public void saveAll(Collection<SplDrug> products,
	                    SqlWriters writers,
	                    IdAllocators ids,
	                    ConcurrentMap<String, Integer> splSrcTracker,
	                    Map<String, Integer> priorGuidMap) {

		ConcurrentMap<String, Integer> newAssignments = new ConcurrentHashMap<>();
		ConcurrentMap<Integer, Set<String>> pidToGuids = new ConcurrentHashMap<>();

		for (SplDrug p : products) {
			if (p != null && p.isSave()) {
				saveOne(p, writers, ids, splSrcTracker, priorGuidMap, newAssignments, pidToGuids);
			}
		}
		writers.flushAll();
		writers.close();
	}

	private void saveOne(SplDrug prd,
	                     SqlWriters w,
	                     IdAllocators ids,
	                     ConcurrentMap<String, Integer> splSrcTracker,
	                     Map<String, Integer> priorGuidMap,
	                     ConcurrentMap<String, Integer> newAssignments,
	                     ConcurrentMap<Integer, Set<String>> pidToGuids) {

		if (!prd.isSave()) return;

		// Assign (or reuse) PRODUCT_ID
		int productId = getOrAssignProductId(prd, priorGuidMap, newAssignments, ids);
		prd.setId(productId);

		// First time we see this PRODUCT_ID? Add SUBSTANCE record.
		Set<String> guids = prd.getMergedGuidXmlPairs() == null ? Set.of()
				: new TreeSet<>(prd.getMergedGuidXmlPairs().keySet());
		Set<String> first = pidToGuids.putIfAbsent(productId, guids);
		if (first == null) {
			println(w.get("PRODUCT"), "INSERT INTO SUBSTANCE (ID) VALUES (" + productId + ");");
		}

		// SRLC + SRC files + NDC + ATC
		saveSrlc(productId, prd, w.get("PROD_RELATED"), splSrcTracker);
		linkSrcFiles(productId, prd, w.get("PROD_RELATED"), splSrcTracker, ids);
		saveNdc(productId, prd, w.get("NDC"), ids);
		saveAtc(productId, prd, w.get("ATC"), ids);

		// RxNorm & SNOMED
		saveRxnorm(productId, prd, w.get("RXNORM"));
		saveSnomed(productId, prd, w.get("SNOMED"));

		// Outcomes (AE / BlackBox / IND), exact+NLP
		writeAe(w.get("AE"), prd.getExactMatchWarnings(), productId, true, false, ids, splSrcTracker);
		writeAe(w.get("AE"), prd.getNlpMatchWarnings(),    productId, true, false, ids, splSrcTracker);
		writeAe(w.get("AE"), prd.getExactMatchBlackbox(),  productId, false, true, ids, splSrcTracker);
		writeAe(w.get("AE"), prd.getNlpMatchBlackbox(),    productId, false, true, ids, splSrcTracker);

		writeInd(w.get("IND"), prd.getExactMatchIndications(), productId, ids, splSrcTracker);
		writeInd(w.get("IND"), prd.getNlpMatchIndications(),   productId, ids, splSrcTracker);
	}

	/** Thread-safe println for writer streams. */
	private static void println(PrintWriter pw, String s) {
		synchronized (pw) {
			pw.println(s);
		}
	}

	/** Pick a prior SUBSTANCE_ID if available; otherwise allocate a new one, stably by representative GUID. */
	private int getOrAssignProductId(SplDrug prd,
	                                 Map<String, Integer> prior,
	                                 ConcurrentMap<String, Integer> newAssign,
	                                 IdAllocators ids) {

		Integer p = canonicalPriorId(prd, prior);
		if (p != null) return p;

		Set<String> guids = prd.getMergedGuidXmlPairs() == null ? Set.of()
				: prd.getMergedGuidXmlPairs().keySet();
		String rep = guids.stream()
				.filter(g -> g != null && !g.isBlank())
				.sorted()
				.findFirst()
				.orElse(null);

		return (rep == null)
				? ids.productId.getAndIncrement()
				: newAssign.computeIfAbsent(rep, k -> ids.productId.getAndIncrement());
	}

	private Integer canonicalPriorId(SplDrug prd, Map<String, Integer> prior) {
		if (prior == null || prd.getMergedGuidXmlPairs() == null) return null;
		Integer min = null;
		for (String g : prd.getMergedGuidXmlPairs().keySet()) {
			Integer id = prior.get(g);
			if (id != null && (min == null || id < min)) min = id;
		}
		return min;
	}

	/** Insert SRLC rows and link them to the product. */
	private void saveSrlc(int productId,
	                      SplDrug prd,
	                      PrintWriter out,
	                      ConcurrentMap<String, Integer> srlcMap) {
		try {
			if (prd.getSrlcs() == null || prd.getSrlcs().isEmpty()) return;

			final String sql = "INSERT INTO SRLC ( DRUG_ID, APPLICATION_NUMBER, DRUG_NAME, ACTIVE_INGREDIENT, SUPPLEMENT_DATE, DATABASE_UPDATED, URL ) values (";
			for (int drugId : prd.getSrlcs().keySet()) {
				if (drugId <= 0) continue;

				var srlc = prd.getSrlcs().get(drugId);
				if (srlc == null) continue;

				String sup = fmt(srlc.getSupplementDate());
				String upd = fmt(srlc.getDatabaseUpdated());
				String unique = Integer.toString(srlc.getDrugId());

				if (!srlcMap.containsKey(unique)) {
					println(out, sql
							+ srlc.getDrugId() + ", "
							+ srlc.getApplicationNumber() + ",\""
							+ escSql(srlc.getDrugName()) + "\", \""
							+ escSql(srlc.getActiveIngredient()) + "\", '"
							+ sup + "', '" + upd + "', \""
							+ escSql(srlc.getUrl()) + "\");");
					srlcMap.put(unique, srlc.getDrugId());
				}
				println(out, "INSERT INTO SUBSTANCE_SRLC ( PRODUCT_ID, DRUG_ID ) values ( "
						+ productId + ", " + srlc.getDrugId() + " );");
			}
		} catch (Exception e) {
			Logger.log("Error adding SRLC data for product: " + productId);
		}
	}

	/** Insert SPL source-file links for each GUID/XML relpath; track canonical SRC_ID per GUID. */
	private void linkSrcFiles(int productId,
	                          SplDrug prd,
	                          PrintWriter out,
	                          ConcurrentMap<String, Integer> splTracker,
	                          IdAllocators ids) {

		final String prefix = "INSERT INTO SPL_SRCFILE (ID, PRODUCT_ID, GUID, XMLFILE_NAME, SOURCE_TYPE_ID, APPLICATION_NUMBER, NDA_SPONSOR, APPROVAL_DATE) VALUES (";

		if (prd.getGuidXmlMaps() == null || prd.getGuidXmlMaps().isEmpty()) return;

		for (String guid : prd.getGuidXmlMaps().keySet()) {
			int appNo = prd.getGuidNda().getOrDefault(guid, 0);
			String date = prd.getGuidApprovalDate().containsKey(guid)
					? fmt(prd.getGuidApprovalDate().get(guid))
					: null;

			boolean haveCanonical = splTracker.containsKey(guid);
			List<String> xmlFiles = prd.getGuidXmlMaps().get(guid);
			if (xmlFiles == null || xmlFiles.isEmpty()) continue;

			String splDataPath = cfg.getSplPath();
			for (String xml : xmlFiles) {
				if (StringUtils.isBlank(xml)) continue;

				// store relpath
				xml = xml.replace(splDataPath, "");

				int srcId = ids.splSrcId.getAndIncrement();
				if (!haveCanonical) {
					splTracker.put(guid, srcId);
					haveCanonical = true;
				}
				Integer srcTypeId = resolveSourceTypeIdFromXmlRelPath(xml);

				String row = prefix + srcId + ", " + productId + ", '" + guid + "', '"
						+ xml + "', "
						+ (srcTypeId == null ? "NULL" : srcTypeId) + ", "
						+ appNo + ", "
						+ (prd.getDrugSponsor() == null ? "NULL" : "'" + escSql(prd.getDrugSponsor()) + "', ")
						+ (date == null ? "NULL" : "'" + date + "'") + ");";
				println(out, row);
			}
		}
	}

	/** Insert NDC codes and PRODUCT↔NDC links, deduping by code where possible. */
	private void saveNdc(int productId, SplDrug prd, PrintWriter out, IdAllocators ids) {
		if (prd.getDrugProduct() == null || prd.getDrugProduct().isEmpty()) return;

		for (String aui : prd.getDrugProduct().keySet()) {
			Atom entry = prd.getDrugProduct().get(aui);
			if (entry == null) continue;

			String ndcCode = entry.getCode();
			String ndcName = entry.getTerm();
			int ndcId = ensureNdcId(aui, ndcCode, ndcName, out, ids); // fills ndcIds & ndcByCode

			if (ndcId > 0) {
				println(out, "INSERT INTO PRODUCT_NDC (PRODUCT_ID, NDC_ID) VALUES ("
						+ productId + ", " + ndcId + ");");
			}
		}
	}

	/**
	 * Ensure an NDC record exists and return its ID. Prefers global de-duplication
	 * by NDC code. When code is missing, falls back to de-dup by (AUI + name).
	 */
	private int ensureNdcId(String aui, String ndcCode, String ndcName, PrintWriter out, IdAllocators ids) {
		final String code = ndcCode == null ? "" : ndcCode.trim();
		final String name = ndcName == null ? "" : ndcName.trim();

		// Prefer uniqueness per NDC code
		if (!code.isEmpty()) {
			return ids.ndcByCode.computeIfAbsent(code, c -> {
				int newId = ids.nextNdcId();
				// Stable hash for the code row: based on (code + name), not AUI.
				String hash = sha256Hex((c + "|" + name).toLowerCase(Locale.ROOT));
				println(out, "INSERT INTO NDC_CODE (ID, NDC_CODE, PRODUCT_NAME, PRODUCT_NAME_HASH) VALUES ("
						+ newId + ", '" + escSql(c) + "', \"" + escSql(name) + "\", '" + hash + "');");
				// Also pre-fill ndcIds to prevent duplicate inserts via the fallback path
				ids.ndcIds.putIfAbsent(aui + "|" + c + "|" + hash, newId);
				return newId;
			});
		}

		// Fallback when code is missing: de-dupe by (AUI + name)
		final String hash = sha256Hex(((aui == null ? "" : aui) + "|" + name).toLowerCase(Locale.ROOT));
		final String mapKey = (aui == null ? "" : aui) + "|" + code + "|" + hash;

		return ids.ndcIds.computeIfAbsent(mapKey, k -> {
			int newId = ids.nextNdcId();
			println(out, "INSERT INTO NDC_CODE (ID, NDC_CODE, PRODUCT_NAME, PRODUCT_NAME_HASH) VALUES ("
					+ newId + ", '" + escSql(code) + "', \"" + escSql(name) + "\", '" + hash + "');");
			return newId;
		});
	}

	/** Simple SQL string escaper for both single & double quotes used in emitted SQL. */
	private static String escSql(String s) {
		if (s == null) return "";
		// Double single-quotes for string literals; escape double-quotes for our emitted SQL strings.
		return s.replace("'", "''").replace("\"", "\\\"");
	}

	/** 64-char hex SHA-256 for PRODUCT_NAME_HASH. */
	private static String sha256Hex(String input) {
		try {
			MessageDigest md = MessageDigest.getInstance("SHA-256");
			byte[] bytes = md.digest(input == null ? new byte[0] : input.getBytes(StandardCharsets.UTF_8));
			StringBuilder sb = new StringBuilder(bytes.length * 2);
			for (byte b : bytes) {
				sb.append(Character.forDigit((b >>> 4) & 0xF, 16));
				sb.append(Character.forDigit(b & 0xF, 16));
			}
			return sb.toString();
		} catch (Exception e) {
			// Extremely unlikely; fallback is empty hash (still stable with code in key).
			return "";
		}
	}

	/** Insert ATC links; ensure there is an NDC row for each referenced code. */
	private void saveAtc(int productId, SplDrug prd, PrintWriter out, IdAllocators ids) {
		if (prd.getAtcCodes() == null || prd.getAtcCodes().isEmpty()) return;

		for (Map.Entry<String, Atom> e : prd.getAtcCodes().entrySet()) {
			String ndcCode = e.getKey();     // key is NDC code
			Atom atc = e.getValue();         // value holds the ATC atom (with databaseId)
			if (atc == null) continue;

			Integer ndcId = ids.ndcByCode.get(ndcCode);
			if (ndcId == null) {
				// We never inserted an NDC row for this code yet; create a minimal one.
				ndcId = ensureNdcId("", ndcCode, "", out, ids);
			}

			println(out, "INSERT INTO SUBSTANCE_ATC (PRODUCT_ID, NDC_ID, ATC_ID) VALUES ("
					+ productId + ", " + ndcId + ", " + atc.getDatabaseId() + ");");
		}
	}

	/** Insert RxNorm links (unique by database ID). */
	private void saveRxnorm(int productId, SplDrug prd, PrintWriter out) {
		if (prd.getRxNormPts() == null || prd.getRxNormPts().isEmpty()) return;

		Set<Integer> seen = new HashSet<>();
		for (Atom a : prd.getRxNormPts().values()) {
			if (a == null) continue;

			Atom canon = umls.getRxNorm().get(a.getAui());
			if (canon == null) continue;

			int rxDbId = canon.getDatabaseId();
			if (rxDbId <= 0) continue;

			if (seen.add(rxDbId)) {
				println(out, "INSERT INTO SUBSTANCE_RXNORM (PRODUCT_ID, RXNORM_ID) VALUES ("
						+ productId + ", " + rxDbId + ");");
			}
		}
	}

	/** Insert SNOMED ingredient/PT/parent links for the product. */
	private void saveSnomed(int productId, SplDrug prd, PrintWriter out) {
		if (prd.getIngredients() != null) {
			for (String aui : prd.getIngredients().keySet()) {
				Atom e = umls.getSnomed().get(aui);
				if (e != null) {
					println(out, "INSERT INTO SUBSTANCE_INGREDIENT (PRODUCT_ID, SNOMED_ID) VALUES ("
							+ productId + ", " + e.getDatabaseId() + ");");
				}
			}
		}
		if (prd.getSnomedPts() != null) {
			for (String aui : prd.getSnomedPts().keySet()) {
				Atom e = umls.getSnomed().get(aui);
				if (e != null) {
					println(out, "INSERT INTO SUBSTANCE_SNOMED_PT (PRODUCT_ID, SNOMED_ID) VALUES ("
							+ productId + ", " + e.getDatabaseId() + ");");
				}
			}
		}
		if (prd.getSnomedParentAuis() != null) {
			for (String aui : prd.getSnomedParentAuis()) {
				Atom e = umls.getSnomed().get(aui);
				if (e != null) {
					println(out, "INSERT INTO SUBSTANCE_SNOMED_PARENT (PRODUCT_ID, SNOMED_ID) VALUES ("
							+ productId + ", " + e.getDatabaseId() + ");");
				}
			}
		}
	}

	/** Emit PRODUCT_AE rows (exact or NLP) and their SRC links. */
	private void writeAe(PrintWriter out,
	                     Outcome o,
	                     int productId,
	                     boolean isWarn,
	                     boolean isBox,
	                     IdAllocators ids,
	                     ConcurrentMap<String, Integer> srcFiles) {

		if (o == null || o.getCodes() == null || o.getCodes().isEmpty()) return;

		int exact = o.isExactMatch() ? 1 : 0;
		for (Atom code : o.getCodes()) {
			if (code == null) continue;

			Date d = o.getFirstAdded().get(code.getAui());
			String ds = (d == null) ? null : fmt(d);

			int aeId = ids.productAeId.getAndIncrement();
			String sql = "INSERT INTO PRODUCT_AE (ID, PRODUCT_ID, MEDDRA_ID, LABEL_DATE, WARNING, BLACKBOX, EXACT_MATCH) VALUES ("
					+ aeId + ", " + productId + ", " + code.getDatabaseId() + ", "
					+ (ds == null ? "NULL" : "'" + ds + "'") + ", "
					+ (isWarn ? 1 : 0) + ", " + (isBox ? 1 : 0) + ", " + exact + ");";
			println(out, sql);

			// Link to the SPL source files from which this AUI was derived
			HashMap<String, List<String>> splSrcFiles = o.getOutcomeSource();
			for (String zipFile : splSrcFiles.keySet()) {
				List<String> auiSet = splSrcFiles.get(zipFile);
				if (auiSet != null && auiSet.contains(code.getAui())) {
					Integer srcId = srcFiles.get(zipFile);
					if (srcId == null) {
						Logger.log("AE link skipped; missing SRC_ID for key: " + zipFile
								+ " (PRODUCT_ID=" + productId + ")");
						continue;
					}
					println(out, "INSERT INTO PRODUCT_AE_SRC (AE_ID, SRC_ID) VALUES (" + aeId + ", " + srcId + ");");
				}
			}
		}
	}

	/** Emit PRODUCT_IND rows (exact or NLP) and their SRC links. */
	private void writeInd(PrintWriter out,
	                      Outcome ind,
	                      int productId,
	                      IdAllocators ids,
	                      ConcurrentMap<String, Integer> srcFiles) {

		if (ind == null || ind.getCodes() == null || ind.getCodes().isEmpty()) return;

		int exact = ind.isExactMatch() ? 1 : 0;
		for (Atom code : ind.getCodes()) {
			if (code == null) continue;

			Date d = ind.getFirstAdded().get(code.getAui());
			String ds = (d == null) ? null : fmt(d);

			int id = ids.productIndId.getAndIncrement();
			String sql = "INSERT INTO PRODUCT_IND (ID, PRODUCT_ID, MEDDRA_ID, LABEL_DATE, EXACT_MATCH) VALUES ("
					+ id + ", " + productId + ", " + code.getDatabaseId() + ", "
					+ (ds == null ? "NULL" : "'" + ds + "'") + ", " + exact + ");";
			println(out, sql);

			// Link to SPL source files
			HashMap<String, List<String>> splSrcFiles = ind.getOutcomeSource();
			for (String zipFile : splSrcFiles.keySet()) {
				List<String> auiSet = splSrcFiles.get(zipFile);
				if (auiSet != null && auiSet.contains(code.getAui())) {
					Integer srcId = srcFiles.get(zipFile);
					if (srcId == null) {
						Logger.log("IND link skipped; missing SRC_ID for key: " + zipFile
								+ " (PRODUCT_ID=" + productId + ")");
						continue;
					}
					println(out, "INSERT INTO PRODUCT_IND_SRC (IND_ID, SRC_ID) VALUES (" + id + ", " + srcId + ");");
				}
			}
		}
	}

	/**
	 * Map XML relative path → SOURCE_TYPE_ID for database table.
	 *  - prescription → 1
	 *  - other        → 2
	 *  - otc          → 3
	 */
	private static Integer resolveSourceTypeIdFromXmlRelPath(String xmlRelPath) {
		if (xmlRelPath == null) return null;
		String s = xmlRelPath.toLowerCase(Locale.ROOT);
		if (s.contains("prescription")) return 1;
		if (s.contains("other"))        return 2;
		if (s.contains("otc"))          return 3;
		return null;
	}
}
