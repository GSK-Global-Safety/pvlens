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
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import org.pvlens.spl.conf.ConfigLoader;
import org.pvlens.spl.umls.Atom;

/**
 * Generates CSV reference files from the PVLens database.
 *
 * Order of generation:
 *  1) Terminology exports (MedDRA, RxNorm, SNOMED)
 *  2) Substance list
 *  3) GUID ↔ source-file mappings
 *  4) Substance ↔ NDC mappings
 *  5) Labeled AEs
 *  6) Labeled Indications
 *  7) Substance ↔ RxNorm links
 *  8) Substance ↔ SNOMED links (PT, Parent, Ingredient)
 */
public class CreateDrugReferenceSet {

	// Database connection details
	private static String DB_HOST;
	private static String DB_PORT;
	private static String DB_USER;
	private static String DB_PASS;
	private static String DB_NAME;

	// Singleton model
	private static CreateDrugReferenceSet instance;

	// Cached terminology (keyed by DB ID)
	private final HashMap<Integer, Atom> meddra = new HashMap<>();
	private final HashMap<Integer, Atom> snomed = new HashMap<>();
	private final HashMap<Integer, Atom> rxnorm = new HashMap<>();

	// Directory to save CSV files (from config)
	private static String CSV_OUTPUT_DIR;

	private CreateDrugReferenceSet() {
		ConfigLoader configLoader = new ConfigLoader();

		DB_HOST = configLoader.getDbHost();
		DB_PORT = configLoader.getDbPort();
		DB_USER = configLoader.getDbUser();
		DB_PASS = configLoader.getDbPass();
		DB_NAME = configLoader.getDbName();

		CSV_OUTPUT_DIR = configLoader.getCsvOutputPath();
		ensureOutDir();
	}

	/** Provides the singleton instance. */
	public static synchronized CreateDrugReferenceSet getInstance() {
		if (instance == null) {
			instance = new CreateDrugReferenceSet();
		}
		return instance;
	}

	/** Main entry: generate all outputs in the standard order. */
	public void generateOutputFiles() {
		Logger.log("Generating CSV map files for PVLens");

		Logger.log("  Generating terminology files...");
		loadMeddra();
		loadRxnorm();
		loadSnomed();

		Logger.log("  Saving substances...");
		generateSubstanceFile();

		Logger.log("  Saving GUID maps...");
		generateGuidMapFile();

		Logger.log("  Saving NDC maps...");
		generateSubstanceToNDCFile();

		Logger.log("  Saving labelled AEs...");
		generateSubstanceListedAEFile();

		Logger.log("  Saving labelled Indications...");
		generateSubstanceListedIndicationFile();

		Logger.log("  Create RxNorm linkages...");
		generateSubstanceToRxnormFile();

		Logger.log("  Create SNOMED linkages...");
		generateSubstanceToSNOMEDFile();

		Logger.log("CSV map files complete");
	}

	/** Create the output directory if it does not exist. */
	private static void ensureOutDir() {
		if (CSV_OUTPUT_DIR == null || CSV_OUTPUT_DIR.isBlank()) {
			CSV_OUTPUT_DIR = "./";
		}
		File dir = new File(CSV_OUTPUT_DIR);
		if (!dir.exists() && !dir.mkdirs()) {
			Logger.warn("Could not create output directory: {}", CSV_OUTPUT_DIR);
		}
		if (!CSV_OUTPUT_DIR.endsWith("/") && !CSV_OUTPUT_DIR.endsWith("\\")) {
			CSV_OUTPUT_DIR += File.separator;
		}
	}

	/** Open a JDBC connection. */
	private static Connection getDatabaseConnection() {
		try {
			String url = "jdbc:mysql://" + DB_HOST + ":" + DB_PORT + "/" + DB_NAME
					+ "?useServerPrepStmts=true&useCursorFetch=true&rewriteBatchedStatements=true";
			return DriverManager.getConnection(url, DB_USER, DB_PASS);
		} catch (Exception e) {
			Logger.error("Error getting database connection: {}", e.getMessage());
			return null;
		}
	}

	/** Create a UTF-8 PrintWriter for a file in the output directory. */
	private static PrintWriter openWriter(String filename) throws Exception {
		return new PrintWriter(CSV_OUTPUT_DIR + filename, StandardCharsets.UTF_8);
	}

	private void loadMeddra() {
		try (PrintWriter out = openWriter("meddra.csv");
		     Connection conn = getDatabaseConnection()) {

			if (conn == null) return;

			out.println("ID|AUI|CUI|CODE|PT_CODE|TERM|TTY");

			String sql = "SELECT ID, MEDDRA_AUI, MEDDRA_CUI, MEDDRA_CODE, MEDDRA_PTCODE, MEDDRA_TERM, MEDDRA_TTY "
					+ "FROM pvlens.MEDDRA;";
			try (PreparedStatement ps = conn.prepareStatement(sql);
			     ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					int id = rs.getInt("ID");
					Atom atom = new Atom();
					atom.setDatabaseId(id);
					atom.setAui(rs.getString("MEDDRA_AUI"));
					atom.setCui(rs.getString("MEDDRA_CUI"));
					atom.setCode(rs.getString("MEDDRA_CODE"));
					atom.setPtCode(rs.getString("MEDDRA_PTCODE"));
					atom.setTerm(toLowerTrim(rs.getString("MEDDRA_TERM")));
					atom.setTty(rs.getString("MEDDRA_TTY"));

					meddra.put(id, atom);
					out.println(atom.toMdrCsvString("|"));
				}
			}
		} catch (Exception e) {
			Logger.error("loadMeddra failed: {}", e.getMessage());
		}
	}

	private void loadSnomed() {
		try (PrintWriter out = openWriter("snomed.csv");
		     Connection conn = getDatabaseConnection()) {

			if (conn == null) return;

			out.println("ID|AUI|CUI|CODE|TERM|TTY");

			String sql = "SELECT ID, AUI, CUI, CODE, TERM, TTY FROM pvlens.SNOMED;";
			try (PreparedStatement ps = conn.prepareStatement(sql);
			     ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					int id = rs.getInt("ID");
					Atom atom = new Atom();
					atom.setDatabaseId(id);
					atom.setAui(rs.getString("AUI"));
					atom.setCui(rs.getString("CUI"));
					atom.setCode(rs.getString("CODE"));
					atom.setTerm(toLowerTrim(rs.getString("TERM")));
					atom.setTty(rs.getString("TTY"));

					snomed.put(id, atom);
					out.println(atom.toCsvString("|"));
				}
			}
		} catch (Exception e) {
			Logger.error("loadSnomed failed: {}", e.getMessage());
		}
	}

	private void loadRxnorm() {
		try (PrintWriter out = openWriter("rxnorm.csv");
		     Connection conn = getDatabaseConnection()) {

			if (conn == null) return;

			out.println("ID|AUI|CUI|CODE|TERM|TTY");

			String sql = "SELECT ID, AUI, CUI, CODE, TERM, TTY FROM pvlens.RXNORM;";
			try (PreparedStatement ps = conn.prepareStatement(sql);
			     ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					int id = rs.getInt("ID");
					Atom atom = new Atom();
					atom.setDatabaseId(id);
					atom.setAui(rs.getString("AUI"));
					atom.setCui(rs.getString("CUI"));
					atom.setCode(rs.getString("CODE"));
					atom.setTerm(toLowerTrim(rs.getString("TERM")));
					atom.setTty(rs.getString("TTY"));

					rxnorm.put(id, atom);
					out.println(atom.toCsvString("|"));
				}
			}
		} catch (Exception e) {
			Logger.error("loadRxnorm failed: {}", e.getMessage());
		}
	}

	private void generateSubstanceFile() {
		try (PrintWriter out = openWriter("substance.csv");
		     Connection conn = getDatabaseConnection()) {

			if (conn == null) return;

			out.println("SUBSTANCE_ID");

			String sql = "SELECT ID FROM pvlens.SUBSTANCE ORDER BY ID ASC;";
			try (PreparedStatement ps = conn.prepareStatement(sql);
			     ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					out.println(rs.getInt("ID"));
				}
			}
		} catch (Exception e) {
			Logger.error("generateSubstanceFile failed: {}", e.getMessage());
		}
	}

	private void generateSubstanceToNDCFile() {
		String sql = """
				SELECT DISTINCT p.PRODUCT_ID, n.NDC_CODE, n.PRODUCT_NAME
				  FROM pvlens.PRODUCT_NDC p
				  JOIN pvlens.NDC_CODE n ON n.ID = p.NDC_ID
				  JOIN pvlens.SUBSTANCE s ON p.PRODUCT_ID = s.ID
				 ORDER BY PRODUCT_ID, NDC_CODE ASC
				""";
		try (PrintWriter out = openWriter("substance_ndc.csv");
		     Connection conn = getDatabaseConnection()) {

			if (conn == null) return;

			out.println("SUBSTANCE_ID|NDC_CODE|PRODUCT_NAME");

			try (PreparedStatement ps = conn.prepareStatement(sql);
			     ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					int substanceId = rs.getInt("PRODUCT_ID");
					String ndc = rs.getString("NDC_CODE");
					String name = rs.getString("PRODUCT_NAME");
					out.println(substanceId + "|" + ndc + "|" + name);
				}
			}
		} catch (Exception e) {
			Logger.error("generateSubstanceToNDCFile failed: {}", e.getMessage());
		}
	}

	/** Labeled Indications (EXACT_MATCH + LABEL_DATE only; no warn/box fields for IND). */
	private void generateSubstanceListedIndicationFile() {
		String sql = """
				SELECT PRODUCT_ID, MEDDRA_ID, EXACT_MATCH, LABEL_DATE
				  FROM pvlens.PRODUCT_IND
				 ORDER BY PRODUCT_ID, MEDDRA_ID ASC
				""";
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");

		try (PrintWriter out = openWriter("substance_listed_ind.csv");
		     Connection conn = getDatabaseConnection()) {

			if (conn == null) return;

			// FIX: Header previously listed WARNING/BLACKBOX which don't exist for IND rows
			out.println("SUBSTANCE_ID|MEDDRA_ID|AUI|CUI|CODE|PT_CODE|TERM|TTY|EXACT_MATCH|LABEL_DATE");

			try (PreparedStatement ps = conn.prepareStatement(sql);
			     ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					int substanceId = rs.getInt("PRODUCT_ID");
					int mdrId = rs.getInt("MEDDRA_ID");
					int exact = rs.getInt("EXACT_MATCH");
					Date dte = rs.getDate("LABEL_DATE");

					Atom mdr = meddra.get(mdrId);
					if (mdr == null) {
						Logger.warn("Missing MedDRA in cache for ID {}", mdrId);
						continue;
					}
					String dateStr = (dte == null) ? "" : df.format(dte);

					StringBuilder row = new StringBuilder();
					row.append(substanceId).append('|').append(mdrId).append('|')
					   .append(mdr.getAui()).append('|')
					   .append(mdr.getCui()).append('|')
					   .append(mdr.getCode()).append('|')
					   .append(mdr.getPtCode()).append('|')
					   .append(mdr.getTerm()).append('|')
					   .append(mdr.getTty()).append('|')
					   .append(exact).append('|')
					   .append(dateStr);
					out.println(row.toString());
				}
			}
		} catch (Exception e) {
			Logger.error("generateSubstanceListedIndicationFile failed: {}", e.getMessage());
		}
	}

	private void generateSubstanceToRxnormFile() {
		String sql = """
				SELECT PRODUCT_ID, RXNORM_ID
				  FROM pvlens.SUBSTANCE_RXNORM
				 ORDER BY PRODUCT_ID, RXNORM_ID ASC
				""";
		try (PrintWriter out = openWriter("substance_rxnorm.csv");
		     Connection conn = getDatabaseConnection()) {

			if (conn == null) return;

			out.println("SUBSTANCE_ID|RXNORM_ID");

			try (PreparedStatement ps = conn.prepareStatement(sql);
			     ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					int substanceId = rs.getInt("PRODUCT_ID");
					int rxnormId = rs.getInt("RXNORM_ID");
					out.println(substanceId + "|" + rxnormId);
				}
			}
		} catch (Exception e) {
			Logger.error("generateSubstanceToRxnormFile failed: {}", e.getMessage());
		}
	}

	private void generateSubstanceToSNOMEDFile() {
		// PT
		try (PrintWriter out = openWriter("substance_snomed_pt.csv");
		     Connection conn = getDatabaseConnection()) {

			if (conn == null) return;

			out.println("SUBSTANCE_ID|SNOMED_ID");
			String sql = """
					SELECT PRODUCT_ID, SNOMED_ID
					  FROM pvlens.SUBSTANCE_SNOMED_PT
					 ORDER BY PRODUCT_ID, SNOMED_ID ASC
					""";
			try (PreparedStatement ps = conn.prepareStatement(sql);
			     ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					out.println(rs.getInt("PRODUCT_ID") + "|" + rs.getInt("SNOMED_ID"));
				}
			}
		} catch (Exception e) {
			Logger.error("generateSubstanceToSNOMEDFile (PT) failed: {}", e.getMessage());
		}

		// Parent
		try (PrintWriter out = openWriter("substance_snomed_parent.csv");
		     Connection conn = getDatabaseConnection()) {

			if (conn == null) return;

			out.println("SUBSTANCE_ID|SNOMED_ID");
			String sql = """
					SELECT PRODUCT_ID, SNOMED_ID
					  FROM pvlens.SUBSTANCE_SNOMED_PARENT
					 ORDER BY PRODUCT_ID, SNOMED_ID ASC
					""";
			try (PreparedStatement ps = conn.prepareStatement(sql);
			     ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					out.println(rs.getInt("PRODUCT_ID") + "|" + rs.getInt("SNOMED_ID"));
				}
			}
		} catch (Exception e) {
			Logger.error("generateSubstanceToSNOMEDFile (Parent) failed: {}", e.getMessage());
		}

		// Ingredient
		try (PrintWriter out = openWriter("substance_snomed_ingredient.csv");
		     Connection conn = getDatabaseConnection()) {

			if (conn == null) return;

			out.println("SUBSTANCE_ID|SNOMED_ID");
			String sql = """
					SELECT PRODUCT_ID, SNOMED_ID
					  FROM pvlens.SUBSTANCE_INGREDIENT
					 ORDER BY PRODUCT_ID, SNOMED_ID ASC
					""";
			try (PreparedStatement ps = conn.prepareStatement(sql);
			     ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					out.println(rs.getInt("PRODUCT_ID") + "|" + rs.getInt("SNOMED_ID"));
				}
			}
		} catch (Exception e) {
			Logger.error("generateSubstanceToSNOMEDFile (Ingredient) failed: {}", e.getMessage());
		}
	}

	private void generateSubstanceListedAEFile() {
		String sql = """
				SELECT PRODUCT_ID, MEDDRA_ID, WARNING, BLACKBOX, EXACT_MATCH, LABEL_DATE
				  FROM pvlens.PRODUCT_AE
				 ORDER BY PRODUCT_ID, MEDDRA_ID ASC
				""";
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");

		try (PrintWriter out = openWriter("substance_listed_ae.csv");
		     Connection conn = getDatabaseConnection()) {

			if (conn == null) return;

			out.println("SUBSTANCE_ID|MEDDRA_ID|AUI|CUI|CODE|PT_CODE|TERM|TTY|WARNING|BLACKBOX|EXACT_MATCH|LABEL_DATE");

			try (PreparedStatement ps = conn.prepareStatement(sql);
			     ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					int substanceId = rs.getInt("PRODUCT_ID");
					int mdrId = rs.getInt("MEDDRA_ID");
					int warning = rs.getInt("WARNING");
					int box = rs.getInt("BLACKBOX");
					int exact = rs.getInt("EXACT_MATCH");
					Date dte = rs.getDate("LABEL_DATE");

					Atom mdr = meddra.get(mdrId);
					if (mdr == null) {
						Logger.warn("Missing MedDRA in cache for ID {}", mdrId);
						continue;
					}
					String dateStr = (dte == null) ? "" : df.format(dte);

					StringBuilder row = new StringBuilder();
					row.append(substanceId).append('|').append(mdrId).append('|')
					   .append(mdr.getAui()).append('|')
					   .append(mdr.getCui()).append('|')
					   .append(mdr.getCode()).append('|')
					   .append(mdr.getPtCode()).append('|')
					   .append(mdr.getTerm()).append('|')
					   .append(mdr.getTty()).append('|')
					   .append(warning).append('|')
					   .append(box).append('|')
					   .append(exact).append('|')
					   .append(dateStr);
					out.println(row.toString());
				}
			}
		} catch (Exception e) {
			Logger.error("generateSubstanceListedAEFile failed: {}", e.getMessage());
		}
	}

	private void generateGuidMapFile() {
		String sql = "SELECT PRODUCT_ID, GUID, XMLFILE_NAME, SOURCE_TYPE_ID, APPLICATION_NUMBER, APPROVAL_DATE "
				+ "FROM pvlens.SPL_SRCFILE "
				+ "ORDER BY PRODUCT_ID, GUID ASC;";
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");

		try (PrintWriter out = openWriter("substance_guid.csv");
		     Connection conn = getDatabaseConnection()) {

			if (conn == null) return;

			out.println("SUBSTANCE_ID|GUID|XMLFILE_NAME|SOURCE_TYPE|APPLICATION_NUMBER|APPROVAL_DATE");

			try (PreparedStatement ps = conn.prepareStatement(sql);
			     ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					int substanceId = rs.getInt("PRODUCT_ID");
					String guid = rs.getString("GUID");
					String xml = rs.getString("XMLFILE_NAME");
					int srcType = rs.getInt("SOURCE_TYPE_ID");
					int appNumber = rs.getInt("APPLICATION_NUMBER");
					Date appDte = rs.getDate("APPROVAL_DATE");
					String approvalDate = (appDte == null) ? "" : df.format(appDte);

					// FIX: On missing date, keep all 6 fields and leave last empty
					out.println(substanceId + "|" + guid + "|" + xml + "|" + srcType + "|" + appNumber + "|" + approvalDate);
				}
			}
		} catch (Exception e) {
			Logger.error("generateGuidMapFile failed: {}", e.getMessage());
		}
	}

	private static String toLowerTrim(String s) {
		return (s == null) ? "" : s.toLowerCase().trim();
	}
}
