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

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;
import org.pvlens.spl.conf.ConfigLoader;
import org.pvlens.spl.umls.Atom;
import org.pvlens.spl.umls.UmlsLoader;

/**
 * Generates SQL scripts to populate PVLens lookup tables from UMLS content
 * (MedDRA, RxNorm, SNOMED, ATC).
 *
 * Notes: - Output is deterministic (sorted) to make diffs and CI checks stable.
 * - Strings are SQL-escaped defensively. - Avoids NPEs and handles empty maps
 * gracefully. - Allows caller-provided output dir and starting ID (optional
 * overloads).
 *
 * Table schemas assumed: - MEDDRA(ID, MEDDRA_CODE, MEDDRA_PTCODE, MEDDRA_TERM,
 * MEDDRA_TTY, MEDDRA_AUI, MEDDRA_CUI) - RXNORM(ID, AUI, CUI, CODE, TERM, TTY) -
 * SNOMED(ID, AUI, CUI, CODE, TERM, TTY) - ATC(ID, AUI, CUI, CODE, TERM, TTY)
 *
 * If your actual DDL differs, adjust the INSERT templates below accordingly.
 *
 * @author Jeffery Painter
 * @created 2024-Aug-23
 */
public class UmlsTerms {

	// Defaults via config; still allow DI for tests
	private final UmlsLoader umls;
	private final Path outputDir;
	private final int startId;

	// ---- Constructors -------------------------------------------------------

	/** Production constructor using ConfigLoader and UmlsLoader singletons. */
	public UmlsTerms() {
		this(new ConfigLoader(), UmlsLoader.getInstance(), 100);
	}

	/** Visible for tests: inject ConfigLoader + UmlsLoader + startId. */
	public UmlsTerms(ConfigLoader config, UmlsLoader umls, int startId) {
		this.umls = Objects.requireNonNull(umls, "umls loader must not be null");
		this.outputDir = Path.of(Objects.requireNonNull(config, "config must not be null").getSqlOutputPath());
		this.startId = Math.max(0, startId);
	}

	// ---- Public API ---------------------------------------------------------

	/** Create MedDRA SQL file and assign database IDs back to each Atom. */
	public void createMeddraTable() {
		final Path out = outputDir.resolve("meddra.sql");
		Logger.log("Creating MedDRA SQL file: " + out);
		final Map<String, Atom> src = safeMap(umls.getMedDRA());

		writeSql(out, pw -> {
			int id = startId;

			for (Atom a : sortedValues(src)) {
				// Defensive field extraction with null-to-empty + escaping
				String code = esc(a.getCode());
				String ptCode = esc(a.getPtCode());
				String term = esc(a.getTerm());
				String tty = esc(a.getTty());
				String aui = esc(a.getAui());
				String cui = esc(a.getCui());

				pw.printf(
						"INSERT INTO MEDDRA (ID, MEDDRA_CODE, MEDDRA_PTCODE, MEDDRA_TERM, MEDDRA_TTY, MEDDRA_AUI, MEDDRA_CUI) VALUES (%d, \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\");%n",
						id, code, ptCode, term, tty, aui, cui);

				// Update Atom with assigned DB ID
				a.setDatabaseId(id);
				id++;
			}
		});

		Logger.log("MedDRA SQL file creation complete");
	}

	/** Create RxNorm SQL file and assign database IDs back to each Atom. */
	public void createRxNormTable() {
		final Path out = outputDir.resolve("rxnorm.sql");
		Logger.log("Creating RxNorm SQL file: " + out);
		generateSimpleTable(out, "RXNORM", safeMap(umls.getRxNorm()));
		Logger.log("RxNorm SQL file creation complete");
	}

	/** Create SNOMED SQL file and assign database IDs back to each Atom. */
	public void createSnomedTable() { // fixed method name typo
		final Path out = outputDir.resolve("snomed.sql");
		Logger.log("Creating SNOMED SQL file: " + out);
		generateSimpleTable(out, "SNOMED", safeMap(umls.getSnomed()));
		Logger.log("SNOMED SQL file creation complete");
	}

	/** Create ATC SQL file and assign database IDs back to each Atom. */
	public void createAtcTable() {
		final Path out = outputDir.resolve("atc.sql");
		Logger.log("Creating ATC SQL file: " + out);
		generateSimpleTable(out, "ATC", safeMap(umls.getAtc()));
		Logger.log("ATC SQL file creation complete");
	}

	// ---- Internals ----------------------------------------------------------

	private Map<String, Atom> safeMap(Map<String, Atom> m) {
		return (m == null) ? Map.of() : m;
	}

	private List<Atom> sortedValues(Map<String, Atom> map) {
		Comparator<Atom> cmp = Comparator.comparing((Atom a) -> nullToEmpty(a.getCode()))
				.thenComparing(a -> nullToEmpty(a.getTerm())).thenComparing(a -> nullToEmpty(a.getAui()));

		return map.values().stream().sorted(cmp).toList(); // JDK 16+; use .collect(Collectors.toList()) for older
	}

	private void generateSimpleTable(Path file, String table, Map<String, Atom> src) {
		writeSql(file, pw -> {
			int id = startId;
			for (Atom a : sortedValues(src)) {
				String aui = esc(a.getAui());
				String cui = esc(a.getCui());
				String code = esc(a.getCode());
				String term = esc(a.getTerm());
				String tty = esc(a.getTty());

				pw.printf(
						"INSERT INTO %s (ID, AUI, CUI, CODE, TERM, TTY) VALUES (%d, \"%s\", \"%s\", \"%s\", \"%s\", \"%s\");%n",
						table, id, aui, cui, code, term, tty);
				a.setDatabaseId(id);
				id++;
			}
		});
	}

	/** Shared writer with header + transaction framing. */
	private void writeSql(Path file, SqlWriter writer) {
		try {
			Files.createDirectories(file.getParent());
			try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(file, StandardCharsets.UTF_8))) {
				// Autocommit OFF is fine; if you prefer explicit START TRANSACTION, adjust
				// here.
				pw.println("SET autocommit = OFF;");
				pw.println("-- Generated by UmlsTerms on " + java.time.LocalDateTime.now());
				writer.write(pw);
				pw.println("COMMIT;");
				pw.flush();
			}
		} catch (IOException ioe) {
			Logger.log("Error writing SQL file: " + file + " - " + ioe.getMessage());
			throw new UncheckedIOException(ioe);
		} catch (RuntimeException re) {
			Logger.log("Error generating SQL file: " + file + " - " + re.getMessage());
			throw re;
		}
	}

	// ---- Utilities ----------------------------------------------------------

	/** Minimal SQL string escaper for double-quoted literals in our templates. */
	private static String esc(String s) {
		if (s == null)
			return "";
		// Normalize and prevent control chars; double any existing quotes/backslashes
		String normalized = StringUtils.replaceEach(s, new String[] { "\\", "\"", "\r", "\n" },
				new String[] { "\\\\", "\\\"", " ", " " });
		// Collapse multiple spaces introduced by newline removal
		return normalized.trim();
	}

	private static String nullToEmpty(String s) {
		return (s == null) ? "" : s;
	}

	@FunctionalInterface
	private interface SqlWriter {
		void write(PrintWriter pw);
	}
}
