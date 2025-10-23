package org.pvlens.spl.util;

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

import java.io.File;
import java.io.FileReader;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.pvlens.spl.conf.ConfigLoader;
import org.pvlens.spl.om.Outcome;
import org.pvlens.spl.om.Srlc;
import org.pvlens.spl.processing.support.SrlcExtractor;
import org.pvlens.spl.umls.UmlsLoader;

/**
 * Processes Safety-related Label Changes (SRLC) from FDA: 1) Loads metadata
 * from the CSV you pre-generated. 2) Parses local HTML files (named by DrugID)
 * to extract AE and Boxed Warning text. 3) Runs exact/NLP MedDRA matching on
 * those texts and attaches results to SRLC entries.
 *
 * The heavy HTML download work is performed by an external Python script; this
 * class only reads local artifacts.
 */
public class SrlcProcessor {

	// ---- Config & constants -------------------------------------------------
	private static final ConfigLoader CONFIG = new ConfigLoader();

	// CSV config
	private static final String SRLC_PATH = CONFIG.getSrlcPath();
	private static final String SRLC_CSV = SRLC_PATH + CONFIG.getSrlcLabelChangeFile();

	// Where HTML files live (filenames like "<DrugID>.html")
	private static final String HTML_DIR = SRLC_PATH + "html_download" + File.separator;

	// CSV column names
	private static final String COL_DRUG_NAME = "Drug Name";
	private static final String COL_ACTIVE_INGREDIENT = "Active Ingredient";
	private static final String COL_APP_NUMBER = "Application Number";
	private static final String COL_APP_TYPE = "Application Type";
	private static final String COL_SUPPLEMENT_DATE = "Supplement Date";
	private static final String COL_DB_UPDATED = "Database Updated";
	private static final String COL_LINK = "Link";

	// Date patterns used in CSV and HTML headings
	private static final String DATE_FMT = "MM/dd/yyyy";

	// Text cleanup rules
	private static final String[] REMOVE_STRINGS = new String[] { "Postmarketing Experience",
			"Approved Drug Label (PDF)", "[see Warnings and Precautions (5.10)]",
			"(Additions and/or revisions underlined)", "6 Adverse Reactions", "6.2 Postmarketing Experience", "6.2",
			"[see Boxed Warning, Warnings and Precautions (5.1), and Drug Abuse and Dependence (9.2, 9.3)]",
			"Additions and/or revisions underlined", "(Additions and/or revisions are underlined)",
			"[see Warnings and Precautions (5.8)]", "[see Warnings and Precautions (5.9)]" };

	private enum SectionKind {
		AE, BOXED_WARNING
	}

	private static final SimpleDateFormat CSV_DF = new SimpleDateFormat(DATE_FMT);

	private static java.util.function.Supplier<org.pvlens.spl.processing.support.SrlcExtractor> EXTRACTOR_FACTORY = org.pvlens.spl.processing.support.SrlcExtractor::new;

	private static java.util.function.Supplier<MedDRAProcessor> MDP_FACTORY = () -> new MedDRAProcessor(
			UmlsLoader.getInstance());

	final static MedDRAProcessor sharedMdp = MDP_FACTORY.get();
	final static SrlcExtractor sharedExtractor = EXTRACTOR_FACTORY.get();

	final int cores = Math.max(1, Runtime.getRuntime().availableProcessors());
	// Try fewer threads while we stabilize memory
	final int poolSize = Integer.getInteger("pvlens.srlc.poolSize", Math.min(cores, 4));

	// ---- Public entrypoint --------------------------------------------------
	/**
	 * Loads SRLC metadata, parses HTML to extract AE and Boxed Warning text, runs
	 * MedDRA matching, and returns only SRLCs that actually contain codes.
	 */
	public static List<Srlc> loadSrlcData() {
		Map<Integer, Srlc> byAppNumber = loadSrlcCsv(SRLC_CSV);
		Logger.log("Total SRLC rows: " + byAppNumber.size());

		// ----- Initialize shared (or per-thread) NLP deps once -----
		Logger.log("Loading UMLS & SRLC extractor factories");
		// MedDRAProcessor is typically read-only after construction; but to be safe,
		// keep one per thread. If you *know* it's thread-safe, you can share one
		// instance.
		final ThreadLocal<MedDRAProcessor> MDP_TL = ThreadLocal.withInitial(() -> MDP_FACTORY.get());
		final ThreadLocal<SrlcExtractor> EXTRACTOR_TL = ThreadLocal.withInitial(() -> EXTRACTOR_FACTORY.get());

		// ----- Concurrency control -----
		final int cores = Math.max(1, Runtime.getRuntime().availableProcessors());
		final int poolSize = Math.min(cores, Integer.getInteger("pvlens.srlc.poolSize", cores));

		ExecutorService exec = Executors.newFixedThreadPool(poolSize, r -> {
			Thread t = new Thread(r, "srlc-worker");
			t.setDaemon(true);
			return t;
		});

		// ----- Progress -----
		final long startMs = System.currentTimeMillis();
		final java.util.concurrent.atomic.LongAdder done = new java.util.concurrent.atomic.LongAdder();
		final int total = byAppNumber.size();
		final java.util.concurrent.atomic.AtomicLong nextLogAtMs = new java.util.concurrent.atomic.AtomicLong(
				startMs + 5000L);
		final java.util.concurrent.atomic.AtomicInteger nextPct = new java.util.concurrent.atomic.AtomicInteger(1);

		// ----- Submit tasks -----
		List<java.util.concurrent.Future<?>> futures = new ArrayList<>(total);
		for (Srlc s : byAppNumber.values()) {
			futures.add(exec.submit(() -> {
				try {
					processOneSrlc(s, sharedMdp, sharedExtractor);
				} catch (Throwable t) {
					Logger.log("SRLC processing failed for app=" + s.getApplicationNumber() + ": " + t.getMessage());
				} finally {
					// progress
					done.increment();
					long cur = done.longValue();
					maybeLogOverallProgress(cur, total, startMs, nextLogAtMs, nextPct);
				}

			}));
		}

		// Wait for completion
		exec.shutdown();
		try {
			exec.awaitTermination(7, java.util.concurrent.TimeUnit.DAYS);
		} catch (InterruptedException ie) {
			Thread.currentThread().interrupt();
		}
		// surface any execution exceptions
		for (java.util.concurrent.Future<?> f : futures) {
			try {
				f.get();
			} catch (Exception ignore) {
			}
		}

		// ----- Keep only SRLCs that have codes -----
		List<Srlc> finalSet = new ArrayList<>();
		for (Srlc s : byAppNumber.values()) {
			if (s.hasCodes()) {
				s.resolveLabeledEvents();
				finalSet.add(s);
			}
		}

		long elapsed = System.currentTimeMillis() - startMs;
		Logger.log(String.format("SRLCs with extracted codes: %d", finalSet.size()));
		Logger.log(String.format("SRLC data load complete in %s", formatDuration(elapsed)));
		return finalSet;
	}

	/**
	 * Per-SRLC worker: read HTML, extract AE/BBW by date, run exact + NLP passes.
	 */
	private static void processOneSrlc(Srlc s, MedDRAProcessor mdp, SrlcExtractor srlcXmlParser) {
		int drugId = s.getDrugId();
		if (drugId <= 0)
			return;

		File htmlFile = new File(HTML_DIR + drugId + ".html");
		if (!htmlFile.isFile()) {
			// Comment this if noisy:
			// Logger.log("SRLC HTML not found for DrugID=" + drugId + " (" +
			// htmlFile.getAbsolutePath() + ")");
			return;
		}

		Document doc = parseHtml(htmlFile);
		if (doc == null)
			return;

		// ---- AE ----
		Map<Date, String> aeTextByDate = extractSectionTextByDate(doc, SectionKind.AE);
		if (aeTextByDate != null && !aeTextByDate.isEmpty()) {
			for (Map.Entry<Date, String> e : aeTextByDate.entrySet()) {
				String txt = cleanText(e.getValue());
				if (txt.isEmpty())
					continue;
				Date d = e.getKey();

				// Run exact + nlp passes back-to-back (shared normalization hot in CPU cache)
				Outcome exact = s.getExactAeMatch();
				Outcome nlp = s.getNlpAeMatch();

				srlcXmlParser.processSrlcExtractedTextForAeMatch(s.getApplicationNumber(), "AE", d, true, exact, mdp,
						txt);
				srlcXmlParser.processSrlcExtractedTextForAeMatch(s.getApplicationNumber(), "AE", d, false, nlp, mdp,
						txt);
			}
		}

		// ---- Boxed Warning ----
		Map<Date, String> bwTextByDate = extractSectionTextByDate(doc, SectionKind.BOXED_WARNING);
		if (bwTextByDate != null && !bwTextByDate.isEmpty()) {
			for (Map.Entry<Date, String> e : bwTextByDate.entrySet()) {
				String txt = cleanText(e.getValue());
				if (txt.isEmpty())
					continue;

				// quick skip for removed/obsolete boxes
				String lower = txt.length() <= 4096 ? txt.toLowerCase(Locale.ROOT) : // bound cost
						txt.substring(0, 4096).toLowerCase(Locale.ROOT);
				if (lower.contains("entire box warning") && lower.contains("deleted"))
					continue;

				Date d = e.getKey();
				Outcome exact = s.getExactBlackboxMatch();
				Outcome nlp = s.getNlpBlackboxMatch();

				srlcXmlParser.processSrlcExtractedTextForAeMatch(s.getApplicationNumber(), "BLACKBOX", d, true, exact,
						mdp, txt);
				srlcXmlParser.processSrlcExtractedTextForAeMatch(s.getApplicationNumber(), "BLACKBOX", d, false, nlp,
						mdp, txt);
			}
		}
	}

	// ---- Progress helpers (global) ----
	private static void maybeLogOverallProgress(long done, long total, long startMs,
			java.util.concurrent.atomic.AtomicLong nextLogAtMs, java.util.concurrent.atomic.AtomicInteger nextPct) {
		if (total <= 0L)
			return;
		final long now = System.currentTimeMillis();
		final long minInterval = Long.getLong("pvlens.srlc.progress.minIntervalMs", 5000L);
		final int stepPercent = Integer.getInteger("pvlens.srlc.progress.stepPercent", 1);

		int pct = (int) Math.min(99, Math.max(1, Math.floorDiv(done * 100L, Math.max(1L, total))));
		int tgt = nextPct.get();

		if (pct >= tgt && now >= nextLogAtMs.get()) {
			nextPct.compareAndSet(tgt, Math.min(99, tgt + stepPercent));
			nextLogAtMs.compareAndSet(nextLogAtMs.get(), now + minInterval);

			long elapsed = now - startMs;
			long estTotal = (done == 0) ? 0 : (elapsed * total) / done;
			long eta = Math.max(0, estTotal - elapsed);

			Logger.log(String.format("SRLC processing: %,d / %,d (%d%%), elapsed %s, ETA %s", done, total, pct,
					formatDuration(elapsed), formatDuration(eta)));
		}
	}

	private static String formatDuration(long ms) {
		long s = ms / 1000;
		long h = s / 3600;
		s %= 3600;
		long m = s / 60;
		s %= 60;
		if (h > 0)
			return String.format("%dh %dm %ds", h, m, s);
		if (m > 0)
			return String.format("%dm %ds", m, s);
		return String.format("%ds", s);
	}

	/**
	 * Loads SRLC metadata, parses HTML to extract AE and Boxed Warning text, runs
	 * MedDRA matching, and returns only SRLCs that actually contain codes.
	 */
	public static List<Srlc> old_loadSrlcData() {
		Map<Integer, Srlc> byAppNumber = loadSrlcCsv(SRLC_CSV);
		Logger.log("Total SRLC rows: " + byAppNumber.size());

		// Initialize NLP dependencies once
		Logger.log("Loading UMLS");
		MedDRAProcessor mdp = MDP_FACTORY.get();
		SrlcExtractor srlcXmlParser = EXTRACTOR_FACTORY.get();

		int counter = 0;
		// Walk the SRLCs and enrich from local HTML
		for (Srlc s : byAppNumber.values()) {

			int drugId = s.getDrugId();
			if (drugId <= 0) {
				// Historically you only process if DrugID is resolved
				continue;
			}

			File htmlFile = new File(HTML_DIR + drugId + ".html");
			if (!htmlFile.isFile()) {
				Logger.log("SRLC HTML not found for DrugID=" + drugId + " (" + htmlFile.getAbsolutePath() + ")");
				continue;
			}

			Document doc = parseHtml(htmlFile);
			if (doc == null)
				continue;

			// Extract AE sections by date
			Map<Date, String> aeTextByDate = extractSectionTextByDate(doc, SectionKind.AE);
			for (Map.Entry<Date, String> e : aeTextByDate.entrySet()) {
				Date d = e.getKey();
				String txt = cleanText(e.getValue());
				if (!txt.isEmpty()) {
					Outcome exact = s.getExactAeMatch();
					Outcome nlp = s.getNlpAeMatch();
					srlcXmlParser.processSrlcExtractedTextForAeMatch(s.getApplicationNumber(), "AE", d, true, exact,
							mdp, txt);
					srlcXmlParser.processSrlcExtractedTextForAeMatch(s.getApplicationNumber(), "AE", d, false, nlp, mdp,
							txt);
				}
			}

			// Extract Boxed Warning sections by date
			Map<Date, String> bwTextByDate = extractSectionTextByDate(doc, SectionKind.BOXED_WARNING);
			for (Map.Entry<Date, String> e : bwTextByDate.entrySet()) {
				Date d = e.getKey();
				String raw = e.getValue();
				String txt = cleanText(raw);
				String lower = txt.toLowerCase(Locale.ROOT);
				if (lower.contains("entire box warning") && lower.contains("deleted")) {
					continue;
				}
				if (!txt.isEmpty()) {
					Outcome exact = s.getExactBlackboxMatch();
					Outcome nlp = s.getNlpBlackboxMatch();
					srlcXmlParser.processSrlcExtractedTextForAeMatch(s.getApplicationNumber(), "BLACKBOX", d, true,
							exact, mdp, txt);
					srlcXmlParser.processSrlcExtractedTextForAeMatch(s.getApplicationNumber(), "BLACKBOX", d, false,
							nlp, mdp, txt);
				}
			}
		}

		// Keep only SRLCs that actually produced codes
		List<Srlc> finalSet = new ArrayList<>();
		for (Srlc s : byAppNumber.values()) {
			if (s.hasCodes()) {
				s.resolveLabeledEvents();
				finalSet.add(s);
			}
		}

		Logger.log("SRLCs with extracted codes: " + finalSet.size());
		Logger.log("SRLC data load complete!");
		return finalSet;
	}

	// ---- CSV loading --------------------------------------------------------
	private static Map<Integer, Srlc> loadSrlcCsv(String path) {
		Map<Integer, Srlc> out = new HashMap<>();

		CSVFormat fmt = CSVFormat.DEFAULT.withDelimiter(',').withHeader().withIgnoreHeaderCase().withTrim();

		try (FileReader reader = new FileReader(path, StandardCharsets.UTF_8);
				CSVParser parser = new CSVParser(reader, fmt)) {

			for (CSVRecord r : parser) {
				try {
					String drugName = safe(r, COL_DRUG_NAME);
					String actIng = safe(r, COL_ACTIVE_INGREDIENT);
					String appNumStr = safe(r, COL_APP_NUMBER);
					String supDateStr = safe(r, COL_SUPPLEMENT_DATE);
					String updDateStr = safe(r, COL_DB_UPDATED);
					String url = safe(r, COL_LINK);

					int appNumber = Integer.parseInt(appNumStr);
					Date supDate = parseCsvDate(supDateStr);
					Date updDate = parseCsvDate(updDateStr);

					Srlc s = new Srlc();
					s.setDrugName(drugName);
					s.setActiveIngredient(actIng);
					s.setApplicationNumber(appNumber);
					s.setSupplementDate(supDate);
					s.setDatabaseUpdated(updDate);
					s.setUrl(url);

					out.put(appNumber, s);

				} catch (Exception rowEx) {
					Logger.log("SRLC CSV row skipped (parse error): " + r.toString() + " :: " + rowEx.getMessage());
				}
			}

		} catch (Exception e) {
			Logger.log("Error reading SRLC CSV: " + path + " :: " + e.getMessage());
		}

		return out;
	}

	private static String safe(CSVRecord r, String col) {
		String v = null;
		try {
			v = r.get(col);
		} catch (Exception ignore) {
			/* fall through */ }
		return v == null ? "" : v.trim();
	}

	private static Date parseCsvDate(String s) throws ParseException {
		if (s == null || s.isBlank())
			return null;
		synchronized (CSV_DF) {
			return CSV_DF.parse(s);
		}
	}

	// ---- HTML parsing & extraction ------------------------------------------

	private static Document parseHtml(File f) {
		try {
			return Jsoup.parse(f, "UTF-8");
		} catch (Exception e) {
			Logger.log("Failed to parse HTML: " + f.getAbsolutePath() + " :: " + e.getMessage());
			return null;
		}
	}

	/**
	 * Returns a map of date -> extracted text for the requested section type. The
	 * FDA page structure is: div#accordion containing repeated pairs of:
	 * <h3>Date</h3><div>Content...</div>
	 */
	private static Map<Date, String> extractSectionTextByDate(Document doc, SectionKind kind) {
		Map<Date, String> out = new HashMap<>();
		Elements accordions = doc.select("div#accordion");
		if (accordions.isEmpty())
			return out;

		// We assume a single accordion; if multiple, process all
		for (Element accordion : accordions) {
			List<Element> children = accordion.children();
			for (int i = 0; i < children.size(); i++) {
				Element h3 = children.get(i);
				if (!"h3".equalsIgnoreCase(h3.tagName()))
					continue;

				Date date = parseHeadingDate(h3.ownText());
				if (date == null)
					continue;

				// Next sibling should be the content holder
				if (i + 1 >= children.size())
					break;
				Element content = children.get(++i);

				String text = switch (kind) {
				case AE -> extractRangeUnderH4(content, "6 Adverse Reactions", h -> h.text().startsWith("6"));
				case BOXED_WARNING ->
					extractRangeUnderH4(content, "Boxed Warning", h -> h.text().startsWith("Boxed Warning"));
				};

				if (text != null && !text.isBlank()) {
					out.put(date, text);
				}
			}
		}
		return out;
	}

	private static Date parseHeadingDate(String headingText) {
		if (headingText == null)
			return null;
		String trimmed = headingText.trim();
		if (trimmed.isEmpty())
			return null;
		try {
			synchronized (CSV_DF) {
				return CSV_DF.parse(trimmed);
			}
		} catch (ParseException e) {
			// Non-fatal: structure may differ for some pages
			return null;
		}
	}

	/**
	 * Finds an
	 * <h4>equal to the given label, then concatenates all following sibling
	 * elements until the next
	 * <h4>whose text does not satisfy the continuation predicate.
	 */
	private static String extractRangeUnderH4(Element content, String exactLabel,
			java.util.function.Predicate<Element> continueH4) {
		Elements h4s = content.select("h4");
		StringBuilder buf = new StringBuilder();

		for (Element h4 : h4s) {
			if (exactLabel.equals(h4.text().trim())) {
				Element sib = h4.nextElementSibling();
				while (sib != null) {
					if ("h4".equalsIgnoreCase(sib.tagName())) {
						if (!continueH4.test(sib))
							break;
					}
					buf.append(sib.text()).append('\n');
					sib = sib.nextElementSibling();
				}
				break; // stop after this block
			}
		}
		String out = buf.toString().trim();
		return out.isEmpty() ? null : out;
	}

	// ---- Text cleanup -------------------------------------------------------

	private static String cleanText(String text) {
		if (text == null || text.isBlank())
			return "";
		String t = text;
		for (String r : REMOVE_STRINGS) {
			t = t.replace(r, " ");
		}
		// Normalize common punctuation to spaces, collapse whitespace
		t = t.replaceAll("[…:,]", " ");
		t = t.replaceAll("\\s+", " ").trim();
		return t;
	}
}
