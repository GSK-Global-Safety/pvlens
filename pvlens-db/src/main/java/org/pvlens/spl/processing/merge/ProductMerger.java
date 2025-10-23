package org.pvlens.spl.processing.merge;

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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.lang3.StringUtils;
import org.pvlens.spl.om.SplDrug;
import org.pvlens.spl.om.Srlc;
import org.pvlens.spl.umls.Atom;
import org.pvlens.spl.umls.UmlsLoader;
import org.pvlens.spl.util.Logger;
import org.pvlens.spl.util.UmlsTerms;

/**
 * Performs multi-criteria merging of {@link SplDrug} instances that refer to the
 * same labeled product (or product family). Merge criteria are applied in
 * sequence and include Drug Product CUIs, SPL GUIDs, ATC classification,
 * NDC equality, FDA NDA/BLA tracker, SNOMED parents, and exact RxNorm
 * ingredients, as well as optional historical mappings.
 *
 * <p>Behavior matches prior versions; edits are limited to documentation,
 * comment clarity, and minor readability improvements.</p>
 */
public class ProductMerger {

	private final UmlsLoader umls;

	public ProductMerger(UmlsLoader umls) {
		this.umls = umls;
	}

	/**
	 * Merge and normalize the full product set using multiple signals. Also updates
	 * approval dates, ATC codes, and brand names, and applies optional
	 * historical-substance-ID merges.
	 *
	 * @param all            initial set of products
	 * @param approvalDates  FDA approval dates keyed by NDA/BLA number
	 * @param umlsTerms      helper to persist UMLS support tables
	 * @param priorGuidMap   optional GUID → prior SUBSTANCE_ID mapping
	 * @return merged products (only items flagged {@code isSave()==true})
	 */
	public ConcurrentLinkedQueue<SplDrug> mergeAll(ConcurrentLinkedQueue<SplDrug> all,
	                                               Map<Integer, Date> approvalDates,
	                                               Map<Integer, String> approvalSponsors,
	                                               UmlsTerms umlsTerms,
	                                               Map<String, Integer> priorGuidMap) {

		Logger.log("Merge products that share the same DrugProduct CUI");
		all = mergeProducts(all);

		Logger.log("Merge by GUID");
		all = mergeByAnyGuid(all);

		Logger.log("Merge products that share the same ATC classification");
		all = mergeProductsOnAtcCode(all);

		Logger.log("Merge by NDC code");
		all = mergeProductsOnNdc(all);

		Logger.log("Update approval dates from FDA data file");
		updateApprovalDates(all, approvalDates);

		Logger.log("Update NDA sponsors from FDA data file");
		updateApprovalSponsors(all, approvalSponsors);

		
		// Enrich with ATC and brand names
		updateAtcCodes(all);
		updateBrandNames(all);

		// Merge on prior SUBSTANCE_ID if provided
		if (priorGuidMap != null && !priorGuidMap.isEmpty()) {
			all = mergeProductsOnPriorId(all, priorGuidMap);
		}

		Logger.log("Prune final set removing entries that should not be saved");
		Logger.log("before prune: " + all.size());
		all = pruneAndAssert(all);
		Logger.log("after prune: " + all.size());

		Logger.log("Sanity check...");
		assertUniqueGuids(all);

		return all;
	}

	/**
	 * Save RxNorm/SNOMED/ATC support tables needed by the merged product set.
	 * Should be called once at the end of pipeline processing.
	 */
	public void saveSupportTables(ConcurrentLinkedQueue<SplDrug> all, UmlsTerms umlsTerms) {
		Logger.log("Load and save the subset of RxNorm required for our products");
		umls.loadRxNorm(all);
		umlsTerms.createRxNormTable();

		Logger.log("Load and save the SNOMED active ingredients");
		umls.loadSnomed(all);
		umlsTerms.createSnomedTable();

		// Load and save all ATC codes
		umlsTerms.createAtcTable();
	}

	/**
	 * Merge products that belong to the same drug product family based on DP CUIs.
	 * Strategy:
	 * <ul>
	 *   <li>Merge products sharing a single DP CUI.</li>
	 *   <li>Merge products sharing identical multi-CUI keys.</li>
	 *   <li>Merge single-CUI products into multi-CUI groups that include that CUI.</li>
	 *   <li>Merge by FDA tracker (NDA/BLA).</li>
	 *   <li>Merge by shared single SNOMED parent.</li>
	 *   <li>Finally, merge by exact RxNorm ingredient match.</li>
	 * </ul>
	 */
	private ConcurrentLinkedQueue<SplDrug> mergeProducts(ConcurrentLinkedQueue<SplDrug> allProducts) {

		int productsToRemove;

		// Group by single DP CUI
		Map<String, List<SplDrug>> drugFamilyByCui = new HashMap<>();
		Map<String, List<SplDrug>> multiCuiDrugs = new HashMap<>();

		for (SplDrug drg : allProducts) {
			if (drg.getDrugProductCuis().size() == 1) {
				String cui = drg.getDrugProductCuis().get(0);
				drugFamilyByCui.computeIfAbsent(cui, k -> new ArrayList<>()).add(drg);
			} else if (drg.getDrugProductCuis().size() > 1) {
				String cuiKey = drg.getMultiCuiKey();
				multiCuiDrugs.computeIfAbsent(cuiKey, k -> new ArrayList<>()).add(drg);
			}
		}

		// Merge products sharing a single DP CUI
		productsToRemove = 0;
		for (String cui : drugFamilyByCui.keySet()) {
			List<SplDrug> drugSet = drugFamilyByCui.get(cui);
			for (int i = 0; i < drugSet.size(); i++) {
				SplDrug prd1 = drugSet.get(i);
				if ( StringUtils.isNoneBlank(prd1.getGuid()) && prd1.isSave() ) {
					for (int j = i + 1; j < drugSet.size(); j++) {
						SplDrug prd2 = drugSet.get(j);
						if (!prd1.getGuid().contentEquals(prd2.getGuid()) && prd1.isSave() && prd2.isSave()) {
							if (prd1.mergeProductGroup(prd2)) {
								prd2.setSave(false);
								productsToRemove++;
							}
						}
					}
				} else {
					continue;
				}
			}
		}
		Logger.log(" >> Merged on DP CUI: " + productsToRemove);

		// Merge products that contain identical sets of DP CUIs
		productsToRemove = 0;
		for (String cuiKey : multiCuiDrugs.keySet()) {
			List<SplDrug> drugSet = multiCuiDrugs.get(cuiKey);
			for (int i = 0; i < drugSet.size(); i++) {
				SplDrug prd1 = drugSet.get(i);
				if ( StringUtils.isNoneBlank(prd1.getGuid()) && prd1.isSave() ) {
					for (int j = i + 1; j < drugSet.size(); j++) {
						SplDrug prd2 = drugSet.get(j);
						if (!prd1.getGuid().contentEquals(prd2.getGuid()) && prd1.isSave() && prd2.isSave()) {
							if (prd1.mergeProductGroup(prd2)) {
								prd2.setSave(false);
								productsToRemove++;
							}
						}
					}
				}
			}
		}
		Logger.log(" >> Merged on multi-DP CUI: " + productsToRemove);

		// Merge single-CUI products into multi-CUI groups when covered by the group
		productsToRemove = 0;
		for (String cui : drugFamilyByCui.keySet()) {
			List<SplDrug> singleCuiSet = drugFamilyByCui.get(cui);
			for (String cuiKey : multiCuiDrugs.keySet()) {
				if (cuiKey.contains(cui)) {
					List<SplDrug> multiCuiSet = multiCuiDrugs.get(cuiKey);
					for (SplDrug prd1 : singleCuiSet) {
						if ( StringUtils.isNoneBlank(prd1.getGuid()) && prd1.isSave() ) {
							for (SplDrug prd2 : multiCuiSet) {
								if (!prd1.getGuid().contentEquals(prd2.getGuid()) && prd1.isSave() && prd2.isSave()) {
                                    // Prefer saving multi-CUI over single-CUI
									if (prd2.mergeProductGroup(prd1)) {
										prd1.setSave(false);
										productsToRemove++;
									}
								}
							}
						}
					}
				}
			}
		}
		Logger.log(" >> Merged single DP CUI onto multi-DP CUI: " + productsToRemove);

		// Merge on FDA tracker number (NDA/BLA)
		productsToRemove = 0;
		for (SplDrug prd1 : allProducts) {
			if ( StringUtils.isNoneBlank(prd1.getGuid()) && prd1.isSave() ) {
				for (SplDrug prd2 : allProducts) {
					if (prd2.isSave() && !prd1.getGuid().contentEquals(prd2.getGuid())) {
						int ndaId1 = prd1.getPrimaryNda();
						int ndaId2 = prd2.getPrimaryNda();
						if (ndaId1 != -1 && ndaId1 == ndaId2) {
							if (prd1.mergeProductGroup(prd2)) {
								prd2.setSave(false);
								productsToRemove++;
							}
						}
					}
				}
			}
		}
		Logger.log(" >> Merged on NDA tracker ID: " + productsToRemove);

		// Merge where a single SNOMED parent is shared
		productsToRemove = 0;
		for (SplDrug prd1 : allProducts) {
			if ( StringUtils.isNoneBlank(prd1.getGuid()) && prd1.isSave() ) {
				for (SplDrug prd2 : allProducts) {
					if (prd1 != prd2 && prd2.isSave() && !prd1.getGuid().contentEquals(prd2.getGuid())) {
						if (prd1.hasSnomedParents(prd2)) {
							if (prd1.mergeProductGroup(prd2)) {
								prd2.setSave(false);
								productsToRemove++;
							}
						}
					}
				}
			}
		}
		Logger.log(" >> Merged on SNOMED Parent: " + productsToRemove);

		// Final pass: exact RxNorm ingredient match
		productsToRemove = 0;
		for (SplDrug prd1 : allProducts) {
			if ( StringUtils.isNoneBlank(prd1.getGuid()) && prd1.isSave() ) {
				for (SplDrug prd2 : allProducts) {
					if (prd1 != prd2 && prd2.isSave() && !prd1.getGuid().contentEquals(prd2.getGuid())) {
						if (prd1.hasExactRxNormIngredients(prd2)) {
							if (prd1.mergeProductGroup(prd2)) {
								prd2.setSave(false);
								productsToRemove++;
							}
						}
					}
				}
			}
		}
		Logger.log(" >> Merged on RxNorm Ingredients: " + productsToRemove);

		// Collect final products (re-resolve merged events)
		ConcurrentLinkedQueue<SplDrug> finalProducts = new ConcurrentLinkedQueue<>();
		for (SplDrug spl : allProducts) {
			if (spl.isSave()) {
				spl.resolveLabeledEvents();
				finalProducts.add(spl);
			}
		}

		// Sanity: ensure GUID references are unique across winners
		Map<String, Boolean> guidSeen = new HashMap<>();
		for (SplDrug drg : finalProducts) {
			Map<String, List<String>> guidMap = drg.getGuidXmlMaps();
			for (String guid : guidMap.keySet()) {
				if ( StringUtils.isNoneBlank(guid)  ) {
					if (!guidSeen.containsKey(guid)) {
						guidSeen.put(guid, Boolean.TRUE);
					} else {
						Logger.log("Duplicate SPL GUID found: " + guid + " in drug with GUID: " + drg.getGuid());
					}
				}
			}
		}

		Logger.log("Final products size: " + finalProducts.size());
		return finalProducts;
	}

	/**
	 * Update per-GUID label dates using the FDA approval date map.
	 */
	private void updateApprovalDates(ConcurrentLinkedQueue<SplDrug> allProducts,
	                                 Map<Integer, Date> approvalDates) {
		for (SplDrug drg : allProducts) {
			for (String guid : drg.getGuidNda().keySet()) {
				int nda = drg.getGuidNda().get(guid);
				if (approvalDates.containsKey(nda)) {
					Date dte = approvalDates.get(nda);
					drg.updateGuidDates(dte);
				}
			}
		}
	}



	/**
	 * Update per-GUID label dates using the FDA approval date map.
	 */
	private void updateApprovalSponsors(ConcurrentLinkedQueue<SplDrug> allProducts,
	                                 Map<Integer, String> approvalSponsors) {
		for (SplDrug drg : allProducts) {
			for (String guid : drg.getGuidNda().keySet()) {
				int nda = drg.getGuidNda().get(guid);
				if (approvalSponsors.containsKey(nda)) {
					String sponsor = approvalSponsors.get(nda);
					drg.setDrugSponsor(sponsor);
				}
			}
		}
	}

	
	
	
	/**
	 * Add ATC codes (if available) based on each product's NDC codes.
	 */
	private void updateAtcCodes(ConcurrentLinkedQueue<SplDrug> allProducts) {
		for (SplDrug drug : allProducts) {
			if (!drug.isSave()) continue;

			List<String> ndcCodes = drug.getNdcCodes();
			for (String ndcCode : ndcCodes) {
				// Normalize for matching to ATC
				String normNdc = SplDrug.normalizeNdc(ndcCode);
				List<Atom> atcCodes = umls.getNdcToAtcCodes(normNdc);
				for (Atom atc : atcCodes) {
					drug.getAtcCodes().put(ndcCode, atc);
				}
			}
		}
	}

	/**
	 * Add brand names associated with each product's RxNorm entries (SBD → brand).
	 */
	private void updateBrandNames(ConcurrentLinkedQueue<SplDrug> allProducts) {
		for (SplDrug drug : allProducts) {
			if (!drug.isSave()) continue;

			Map<String, Atom> rxnorm = drug.getRxNormPts();
			if (rxnorm == null || rxnorm.isEmpty()) continue;

			// Snapshot AUIs to avoid mutating while iterating
			List<String> seedAuis = new ArrayList<>(rxnorm.keySet());
			Map<String, Atom> stagedAdds = new LinkedHashMap<>();

			for (String aui : seedAuis) {
				Atom atom = rxnorm.get(aui);
				if (atom == null) continue;

				// Map SBDs to brand names
				if (atom.getTty().contains("SBD")) {
					List<Atom> brands = umls.getBrandNameMatches(atom.getAui());
					if (brands == null || brands.isEmpty()) continue;

					for (Atom bn : brands) {
						if (bn != null && !rxnorm.containsKey(bn.getAui())) {
							stagedAdds.putIfAbsent(bn.getAui(), bn);
						}
					}
				}
			}

			// Apply outside of iteration
			if (!stagedAdds.isEmpty()) {
				for (Atom bn : stagedAdds.values()) {
					drug.addRxNormPt(bn);
				}
			}
		}
	}

	/**
	 * Merge products that share any SPL GUID (primary or within per-product GUID maps).
	 */
	private ConcurrentLinkedQueue<SplDrug> mergeByAnyGuid(ConcurrentLinkedQueue<SplDrug> all) {
		Map<String, SplDrug> ownerByGuid = new HashMap<>();
		int merged = 0;

		for (SplDrug p : all) {
			if (p == null || !p.isSave()) continue;

			// Collect all known GUIDs
			LinkedHashSet<String> guids = new LinkedHashSet<>();
			String g = p.getGuid();
			if (g != null && !g.isBlank()) guids.add(g);

			Map<String, List<String>> m1 = p.getGuidXmlMaps();
			if (m1 != null) guids.addAll(m1.keySet());

			Map<String, List<String>> m2 = p.getMergedGuidXmlPairs();
			if (m2 != null) guids.addAll(m2.keySet());

			if (guids.isEmpty()) continue;

			// Find an existing winner for any of these GUIDs
			SplDrug winner = null;
			for (String guid : guids) {
				SplDrug w = ownerByGuid.get(guid);
				if (w != null && w.isSave()) {
					winner = w;
					break;
				}
			}

			if (winner == null) {
				winner = p;
			} else if (winner != p && winner.isSave() && p.isSave()) {
				if (winner.mergeProductGroup(p)) {
					p.setSave(false);
					merged++;
				}
			}

			// Point all GUIDs at the winner
			for (String guid : guids) {
				ownerByGuid.put(guid, winner);
			}

			winner.setSave(true);
		}

		Logger.log(" >> Merged on duplicate GUID: " + merged);
		return all;
	}

	/**
	 * Merge products that share at least one identical normalized NDC code.
	 */
	private ConcurrentLinkedQueue<SplDrug> mergeProductsOnNdc(ConcurrentLinkedQueue<SplDrug> all) {
		Map<String, SplDrug> winnerByNdc = new HashMap<>();
		int merged = 0;

		for (SplDrug p : all) {
			if (!p.isSave()) continue;

			for (String ndc : p.getNdcCodes()) {
				String norm = SplDrug.normalizeNdc(ndc);
				if (norm == null || norm.isBlank()) continue;

				SplDrug winner = winnerByNdc.putIfAbsent(norm, p);
				if (winner != null && winner != p) {
					// Same NDC → same labeled product; merge into first seen
					if (winner.isSave() && p.isSave() && winner.mergeProductGroup(p)) {
						p.setSave(false);
						merged++;
					}
				}
			}
		}
		Logger.log(" >> Merged on exact NDC: " + merged);
		return all;
	}

	/**
	 * Merge products that share an identical ATC class (exact class equality).
	 */
	private ConcurrentLinkedQueue<SplDrug> mergeProductsOnAtcCode(ConcurrentLinkedQueue<SplDrug> allProducts) {

		int productsToRemove = 0;
		for (SplDrug prd1 : allProducts) {
			for (SplDrug prd2 : allProducts) {
				if (!prd1.getGuid().contentEquals(prd2.getGuid()) && prd1.isSave() && prd2.isSave()) {
					if (prd1.hasExactAtcClass(prd2)) {
						if (prd1.mergeProductGroup(prd2)) {
							// Logger.log("Merged SPL GUID: " + prd2.getGuid() + " with " + prd1.getGuid());
							prd2.setSave(false);
							productsToRemove++;
						}
					}
				}
			}
		}
		Logger.log(" >> Merged on ATC classification: " + productsToRemove);

		ConcurrentLinkedQueue<SplDrug> finalProducts = new ConcurrentLinkedQueue<>();
		for (SplDrug spl : allProducts) {
			if (spl.isSave()) {
				spl.resolveLabeledEvents();
				finalProducts.add(spl);
			}
		}

		// Sanity: ensure GUID references are unique across winners
		Map<String, Boolean> guidSeen = new HashMap<>();
		for (SplDrug drg : finalProducts) {
			Map<String, List<String>> guidMap = drg.getGuidXmlMaps();
			for (String guid : guidMap.keySet()) {
				if (!guidSeen.containsKey(guid)) {
					guidSeen.put(guid, Boolean.TRUE);
				} else {
					Logger.log("Duplicate SPL GUID found: " + guid + " in drug with GUID: " + drg.getGuid());
				}
			}
		}

		Logger.log("Final products size: " + finalProducts.size());
		return finalProducts;
	}

	/**
	 * Merge products that map (via {@code priorGuidMap}) to the same historical
	 * SUBSTANCE_ID. Logs conflicts where a single product maps to multiple IDs.
	 */
	private ConcurrentLinkedQueue<SplDrug> mergeProductsOnPriorId(ConcurrentLinkedQueue<SplDrug> allProducts,
	                                                              Map<String, Integer> priorGuidMap) {

		// priorId → winner product
		Map<Integer, SplDrug> winnerByPriorId = new HashMap<>();
		int mergedCount = 0;
		int conflictCount = 0;

		for (SplDrug p : allProducts) {
			if (!p.isSave()) continue;

			// Collect all prior IDs reachable from this product’s GUIDs
			Set<Integer> priorIds = new HashSet<>();
			for (String guid : p.getMergedGuidXmlPairs().keySet()) {
				Integer pid = priorGuidMap.get(guid);
				if (pid != null) priorIds.add(pid);
			}
			if (priorIds.isEmpty()) continue;

			// If a single product maps to multiple different prior IDs, log a conflict
			if (priorIds.size() > 1) {
				conflictCount++;
				Logger.warn("Product has GUIDs mapped to multiple prior SUBSTANCE_IDs: " + priorIds
						+ " — representative GUIDs: " + p.getMergedGuidXmlPairs().keySet());
			}

			// Choose a canonical prior ID (e.g., the smallest)
			int chosenPriorId = Collections.min(priorIds);

			SplDrug winner = winnerByPriorId.get(chosenPriorId);
			if (winner == null) {
				winnerByPriorId.put(chosenPriorId, p);
			} else if (winner != p) {
				if (winner.mergeProductGroup(p)) {
					p.setSave(false);
					mergedCount++;
				}
			}
		}

		Logger.log("mergeProductsOnPriorId: merged=" + mergedCount + ", conflicts=" + conflictCount);
		return allProducts;
	}

	/**
	 * Assert that GUIDs are uniquely owned among the saved products. Logs up to a
	 * fixed number of conflicts for triage; does not throw to allow pipeline
	 * completion while retaining visibility.
	 */
	private void assertUniqueGuids(Collection<SplDrug> products) {
		Map<String, SplDrug> ownerByGuid = new HashMap<>();
		Map<String, List<String>> conflicts = new LinkedHashMap<>();
		int totalGuidRefs = 0;

		for (SplDrug p : products) {
			if (p == null || !p.isSave()) continue;

			Map<String, List<String>> guidMap = p.getMergedGuidXmlPairs();
			if (guidMap == null || guidMap.isEmpty()) continue;

			for (String guid : guidMap.keySet()) {
				if (guid == null || guid.isBlank()) continue;
				totalGuidRefs++;

				SplDrug prev = ownerByGuid.putIfAbsent(guid, p);
				if (prev != null && prev != p) {
					conflicts.computeIfAbsent(guid, g -> new ArrayList<>(2)).add(describeProduct(prev));
					conflicts.get(guid).add(describeProduct(p));
				}
			}
		}

		Logger.log("GUID uniqueness check: totalRefs=" + totalGuidRefs
				+ ", uniqueGUIDs=" + ownerByGuid.size()
				+ ", conflicts=" + conflicts.size());

		if (!conflicts.isEmpty()) {
			int shown = 0, maxShow = 25;
			for (var e : conflicts.entrySet()) {
				Logger.log("GUID CONFLICT " + e.getKey() + " owned by: " + String.join(" | ", e.getValue()));
				if (++shown >= maxShow) {
					Logger.log("... (" + (conflicts.size() - shown) + " more conflicts not shown)");
					break;
				}
			}
			Logger.error("GUID uniqueness violated: " + conflicts.size() + " conflicted GUIDs");
		}
	}

	/** Brief string summary for logs; robust even before IDs are assigned. */
	private String describeProduct(SplDrug p) {
		String idStr = "id=" + safeGetId(p);
		String obj = "obj@" + System.identityHashCode(p);
		String repGuid = null;
		var m = p.getMergedGuidXmlPairs();
		if (m != null && !m.isEmpty()) repGuid = m.keySet().iterator().next();
		return idStr + ", repGuid=" + repGuid + ", " + obj;
	}

	/** Keep only products flagged for save; log keep/drop counts. */
	private ConcurrentLinkedQueue<SplDrug> pruneAndAssert(Collection<SplDrug> products) {
		ConcurrentLinkedQueue<SplDrug> out = new ConcurrentLinkedQueue<>();
		int kept = 0, dropped = 0;
		for (SplDrug p : products) {
			if (p != null && p.isSave()) {
				out.add(p);
				kept++;
			} else {
				dropped++;
			}
		}
		Logger.log("PruneMergedProducts: kept=" + kept + " dropped=" + dropped);
		return out;
	}

	/** Safe accessor in case {@code getId()} is unset or unavailable. */
	private Integer safeGetId(SplDrug p) {
		try {
			return p.getId();
		} catch (Throwable t) {
			return -1;
		}
	}
}
