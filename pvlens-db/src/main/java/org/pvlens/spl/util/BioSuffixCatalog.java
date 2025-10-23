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
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.pvlens.spl.conf.ConfigLoader;

/**
 * Catalog of FDA biologic suffixes (4-letter codes appended to nonproprietary
 * names).
 *
 * Loads from the Excel file specified by ConfigLoader#getBiosimilarFile().
 * Exposes a human-inspectable suffix->note map and helpers to strip suffixes
 * during token normalization.
 * 
 * For more information see:
 * https://www.centerforbiosimilars.com/view/alphabet-soup-the-story-behind-biosimilar-nonproprietary-name-suffixes
 * 
 */
public final class BioSuffixCatalog {

	private BioSuffixCatalog() {
	}

	/** Ensure init runs once per process (idempotent). */
	private static final AtomicBoolean INIT = new AtomicBoolean(false);

	/**
	 * Lowercase 4-letter suffix -> note (e.g., base nonproprietary name without
	 * suffix).
	 */
	private static volatile Map<String, String> SUFFIXES = Collections.emptyMap();

	/** Extract text in parentheses, e.g. "(insulin aspart-xjhz)". */
	private static final Pattern PAREN = Pattern.compile("\\(([^)]{1,200})\\)");

	/**
	 * Find a trailing "-xxxx" (4 letters) preceded by a letter, at token or group
	 * end.
	 */
	private static final Pattern SUFFIX_TAIL = Pattern.compile("(?i)(?<=[A-Za-z])-([a-z]{4})(?=\\b|$)");
	private static final Pattern FOUR_LETTER = Pattern.compile("^[a-z]{4}$");

	// ------------------------------------------------------------------------
	// Public API
	// ------------------------------------------------------------------------

	/** Initialize once; now merges FDA base + optional *_enriched.(xlsx|csv). */
	public static void init(ConfigLoader cfg) {
		if (cfg == null)
			throw new IllegalArgumentException("ConfigLoader is null");
		if (!INIT.compareAndSet(false, true))
			return;

		String basePath = cfg.getBiosimilarFile(); // e.g. src/main/resources/fda/biosimilar_fda.xlsx
		Map<String, String> base = loadSuffixesFromExcel(basePath);
		int baseN = base.size();

		// Enriched file
		basePath = cfg.getBiosimilarEnrichedFile();
		Map<String, String> merged = new LinkedHashMap<>(base);

		// Try enriched .xlsx (filesystem or classpath)
		Map<String, String> addX = loadSuffixesFromExcel(basePath.toString());
		int addXn = addX.size();
		if (!addX.isEmpty())
			merged.putAll(addX);

		SUFFIXES = Collections.unmodifiableMap(merged);
		Logger.info("BioSuffixCatalog: loaded base=" + baseN + ", enrichedXlsx=" + addXn + " total=" + SUFFIXES.size());
	}

	/** Inspectable map (lowercase suffix -> note). */
	public static Map<String, String> suffixes() {
		return SUFFIXES;
	}

	/** Lowercase set of all suffix keys. */
	public static Set<String> keys() {
		return SUFFIXES.keySet();
	}

	/** True if sfx (4 letters) is in catalog. Case-insensitive. */
	public static boolean isBiologicSuffix(String sfx) {
		if (sfx == null || sfx.length() != 4)
			return false;
		return SUFFIXES.containsKey(sfx.toLowerCase(Locale.ROOT));
	}

	/**
	 * Strip a known biologic suffix from a single token. Only removes trailing
	 * "-xxxx" when xxxx is a known suffix (case-insensitive).
	 */
	public static String stripSuffixFromToken(String token) {
		if (token == null)
			return null;
		int dash = token.lastIndexOf('-');
		if (dash < 0 || dash == token.length() - 1)
			return token;
		String tail = token.substring(dash + 1);
		if (isBiologicSuffix(tail)) {
			return token.substring(0, dash);
		}
		return token;
	}

	/**
	 * Strip suffixes inline in a full text (e.g., “relatlimab-rmbw,
	 * nivolumab-nvhy”). Only removes cataloged suffixes; preserves
	 * punctuation/spacing.
	 */
	public static String stripSuffixesInText(String text) {
		if (text == null || text.isEmpty() || SUFFIXES.isEmpty())
			return text;
		// Replace only when captured group is a known suffix
		Matcher m = SUFFIX_TAIL.matcher(text);
		StringBuffer sb = new StringBuffer(text.length());
		while (m.find()) {
			String sfx = m.group(1).toLowerCase(Locale.ROOT);
			if (SUFFIXES.containsKey(sfx)) {
				m.appendReplacement(sb, ""); // drop the "-xxxx"
			}
		}
		m.appendTail(sb);
		return sb.toString();
	}

	// ------------------------------------------------------------------------
	// Loader
	// ------------------------------------------------------------------------

	private static Map<String, String> loadSuffixesFromExcel(String pathOrResource) {
		Map<String, String> out = new LinkedHashMap<>();
		if (pathOrResource == null || pathOrResource.isBlank()) {
			Logger.error("BioSuffixCatalog: BIOSIMILAR_FILE is blank.");
			return out;
		}

		try (InputStream in = openExcel(pathOrResource)) {
			if (in == null) {
				Logger.error("BioSuffixCatalog: cannot open " + pathOrResource + " (file or resource).");
				return out;
			}
			try (Workbook wb = new XSSFWorkbook(in)) {
				DataFormatter fmt = new DataFormatter(Locale.ROOT);
				Sheet sheet = wb.getNumberOfSheets() > 0 ? wb.getSheetAt(0) : null;
				if (sheet == null)
					return out;

				for (Row row : sheet) {
					// Skip the header-ish first row if it looks like a title
					if (row.getRowNum() == 0)
						continue;

					for (Cell cell : row) {
						String text = fmt.formatCellValue(cell);
						if (text == null || text.isBlank())
							continue;

						// 1) Try to extract “(nonproprietary-suffix)” from parentheses
						Matcher paren = PAREN.matcher(text);
						while (paren.find()) {
							extractFromGroup(paren.group(1), out);
						}

						// 2) Also scan the raw cell for tokens ending with -xxxx (safety net)
						extractFromFreeText(text, out);
					}
				}
			}
		} catch (Exception e) {
			Logger.error("BioSuffixCatalog: failed to read Excel: " + e.getMessage());
		}

		return out;
	}

	private static InputStream openExcel(String pathOrResource) {
		// First try filesystem
		try {
			File f = new File(pathOrResource);
			if (f.exists() && f.isFile() && Files.isReadable(f.toPath())) {
				return new FileInputStream(f);
			}
		} catch (Exception ignore) {
		}

		// Fallback to classpath (strip common src/main/resources prefix)
		String res = pathOrResource;
		if (res.startsWith("src/main/resources/")) {
			res = res.substring("src/main/resources/".length());
		}
		ClassLoader cl = Thread.currentThread().getContextClassLoader();
		InputStream in = (cl != null) ? cl.getResourceAsStream(res)
				: BioSuffixCatalog.class.getResourceAsStream("/" + res);
		return in;
	}

	// Pull suffix and a friendly note from a parenthetical group like "insulin
	// aspart-xjhz"
	private static void extractFromGroup(String group, Map<String, String> out) {
		if (group == null)
			return;
		// find a suffix in the group
		Matcher m = SUFFIX_TAIL.matcher(group);
		while (m.find()) {
			String sfx = m.group(1).toLowerCase(Locale.ROOT);
			if (!FOUR_LETTER.matcher(sfx).matches())
				continue;

			// Try to build a note like "insulin aspart"
			String base = group;
			int dash = base.lastIndexOf('-');
			if (dash > 0)
				base = base.substring(0, dash).trim();
			putIfAbsent(out, sfx, base);
		}
	}

	// As a safety net, scan whole text for ...-xxxx patterns and record them
	private static void extractFromFreeText(String text, Map<String, String> out) {
		Matcher m = SUFFIX_TAIL.matcher(text);
		while (m.find()) {
			String sfx = m.group(1).toLowerCase(Locale.ROOT);
			if (!FOUR_LETTER.matcher(sfx).matches())
				continue;
			putIfAbsent(out, sfx, "from cell");
		}
	}

	private static void putIfAbsent(Map<String, String> out, String sfx, String note) {
		out.putIfAbsent(sfx, note == null ? "" : note);
	}

}
