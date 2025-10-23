package org.pvlens.spl.umls;

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

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.pvlens.spl.conf.ConfigLoader;
import org.pvlens.spl.om.SplDrug;
import org.pvlens.spl.processing.support.NdcExtractor;
import org.pvlens.spl.util.Logger;
import org.pvlens.spl.util.StopwordRemover;

import opennlp.tools.stemmer.snowball.englishStemmer;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;

/**
 * Loads UMLS subsets (MTHSPL, MedDRA, SNOMED CT, RxNorm, ATC) and builds
 * cross-terminology maps to support SPL processing and grouping.
 *
 * Singleton: use {@link #getInstance()}.
 *
 * Author: Jeffery Painter Created: 2024-08-23 Updated: 2025-08-22
 */
public class UmlsLoader {

	// =========================================================================
	// Singleton
	// =========================================================================

	private static UmlsLoader instance;

	public static synchronized UmlsLoader getInstance() {
		if (instance == null) {
			instance = new UmlsLoader();
		}
		return instance;
	}

	// TEST SUPPORT: allow tests to install a custom instance
	static synchronized void setInstanceForTests(UmlsLoader testInstance) {
		instance = testInstance;
	}

	static synchronized void resetInstanceForTests() {
		instance = null;
	}

	private UmlsLoader() {
		// DB config
		DB_HOST = configLoader.getDbHost();
		DB_PORT = configLoader.getDbPort();
		DB_USER = configLoader.getDbUser();
		DB_PASS = configLoader.getDbPass();
		DB_NAME = configLoader.getDbName();
		UMLS_DB_NAME = configLoader.getUmlsDbName();
		DB_DRIVER = configLoader.getDbDriver();

		// NLP init
		this.tokenizerModel = loadTokenizerModel();
		this.tokenizerLocal = ThreadLocal.withInitial(() -> new TokenizerME(this.tokenizerModel));

		// tokenizerFn: default uses OpenNLP
		this.tokenizerFn = s -> this.tokenizerLocal.get().tokenize(s == null ? "" : s);

		// Load reference dictionaries
		loadRxNorm();
		loadATC();
		loadBrandNames();
		loadMedDRA();

		// Precompute transforms for matching (parallelized)
		this.transformedMeddraMap = loadTransformedMeddraMap(false);
		this.stemmedTransformedMeddraMap = loadStemmedTransformedMeddraMap(false);
		return;
	}

	private static final long PROGRESS_MIN_INTERVAL_MS = 5000L;
	private static final int PROGRESS_STEP_PERCENT = 5;
	private static final boolean PROGRESS_ENABLED = false;

	/** Factory for test-only lightweight instances (no DB or NLP models). */
	UmlsLoader(boolean testMode) {
		// give finals a value; we won't use tokenizerLocal in tests
		this.tokenizerModel = null;
		this.tokenizerLocal = ThreadLocal.withInitial(() -> null);

		// default to whitespace tokenizer in tests
		this.tokenizerFn = s -> (s == null || s.isBlank()) ? new String[0] : s.trim().split("\\s+");

		// leave maps empty; tests will seed via _testPut* helpers
		// (no loadATC(), no loadMedDRA(), no precompute)
	}

	// TEST-ONLY: factory for creating a lightweight instance
	public static UmlsLoader newTestInstance() {
		return new UmlsLoader(true);
	}

	private static final Map<String, String> ORGAN_ADJ = Map.of("ovary", "ovarian", "lung", "lung", // already fine
			"breast", "breast", // already fine
			"kidney", "renal", "liver", "hepatic", "stomach", "gastric", "brain", "cerebral", "blood", "hematologic");

	// ---- TEST HOOKS (no-op in prod unless you call them) ------------------
	private java.util.function.Function<String, String[]> _testTokenizer = null;

	/** Inject a simple tokenizer for tests (e.g., whitespace split). */
	public void _testSetTokenizer(java.util.function.Function<String, String[]> tok) {
		this._testTokenizer = tok;
	}

	/** Seed meddraTerms directly for tests. */
	public void _testPutMeddraTerm(String tty, String term, String... auis) {
		Map<String, List<String>> ttyMap = meddraTerms.computeIfAbsent(tty, k -> new HashMap<>());
		ttyMap.computeIfAbsent(term, k -> new ArrayList<>()).addAll(Arrays.asList(auis));
	}

	/** Clear & rebuild transformed maps from current meddraTerms (tests). */
	public void _testRebuildTransformedMaps() {
		this.transformedMeddraMap = new HashMap<>();
		this.stemmedTransformedMeddraMap = new HashMap<>();
		loadTransformedMeddraMap(false);
		loadStemmedTransformedMeddraMap(false);
	}

	// =========================================================================
	// Config / constants
	// =========================================================================

	private static final ConfigLoader configLoader = new ConfigLoader();
	private final StopwordRemover stopword = StopwordRemover.getInstance();

	private static String DB_HOST;
	private static String DB_PORT;
	private static String DB_USER;
	private static String DB_PASS;
	private static String DB_NAME; // app db (not used here)
	private static String UMLS_DB_NAME; // umls schema/db
	private static String DB_DRIVER;

	private static final String TOKEN_MODEL_PATH = "models/en-token.bin";

	/** Valid MedDRA TTYs (preference order documented). */
	private static final List<String> VALID_TTY = Collections
			.unmodifiableList(Arrays.asList("PT", /* "LT", */ "LLT", "HT", "HG", "OS"));

	/** Included UMLS semantic types (MRSTY.STN prefixes), SIDER-aligned. */
	private static final Set<String> INCLUDED_SEMANTIC_TYPES = Collections
			.unmodifiableSet(new HashSet<>(Arrays.asList("A1.2.2", "A2.2", "B2.2.1.1", "B2.2.1.2", "B2.3", "B1.1.2")));

	private static final Locale ROOT = Locale.ROOT;

	private boolean GUID_MAP_LOADED = false;

	public static String EXAMPLE_NDC = "00006-0705-68";

	// tokenizerFn (injectable for tests)
	private Function<String, String[]> tokenizerFn; // <== add

	// --- SQL: constants -------------------------------------------------------

	// ATC
	private static final String SQL_ATC_ATOMS = """
			SELECT AUI, CUI, TTY, STR, CODE
			FROM MRCONSO
			WHERE SAB = 'ATC'
			""";

	// ATC
	private static final String SQL_RXNORM_ATOMS = """
			SELECT AUI, CUI, TTY, STR, CODE
			FROM MRCONSO
			WHERE SAB = 'RXNORM'
			""";

	// Extract all NDC codes
	private static final String SQL_NDC_TO_CUI = """
			SELECT DISTINCT sat.CUI, sat.ATV FROM umls.MRSAT sat WHERE sat.ATN = 'NDC';
			""";

	private static final String SQL_RXCUI_TO_NDC = """
			SELECT DISTINCT conso.CUI AS RX_CUI, conso.STR AS RX_STR, sat.ATV AS NDC
			FROM umls.MRCONSO conso, umls.MRSAT sat
			WHERE conso.AUI = sat.METAUI
			  AND conso.TTY IN ('SCD','SBD')
			  AND sat.SAB = 'RXNORM'
			  AND sat.ATN = 'NDC'
			""";

	/** extract brand names and link to RxNorm SBD and SBDC codes **/
	private static final String SQL_BN_TO_RXCUI = """
			SELECT DISTINCT
			            bn.CUI        AS brand_cui,
			            bn.AUI        AS brand_aui,
			            bn.CODE       AS brand_code,
			            bn.STR        AS brand_name,
			            ing.CUI       AS linked_cui,
			            ing.AUI       AS linked_aui,
			            ing.CODE      AS linked_code,
			            ing.TTY       AS linked_tty,
			            ing.STR       AS linked_name
			          FROM umls.MRCONSO AS bn
			          JOIN umls.MRREL   AS r
			            ON r.SAB  = 'RXNORM'
			           AND r.RELA = 'ingredient_of'
			           AND r.CUI2 = bn.CUI
			          JOIN umls.MRCONSO AS ing
			            ON ing.SAB = 'RXNORM'
			           AND ing.CUI = r.CUI1
			          WHERE bn.SAB='RXNORM' AND bn.TTY='BN'
			            AND bn.LAT='ENG' AND bn.SUPPRESS='N'
			            AND ing.LAT='ENG' AND ing.SUPPRESS='N'
			            AND ing.TTY IN ('SBD', 'SBDC' )
			          ORDER BY bn.STR, ing.STR;
				""";

	private static final String SQL_MTHSPL_REL_RO = """
			SELECT DISTINCT rel.CUI1, rel.CUI2
			FROM umls.MRREL rel
			WHERE rel.SAB = 'MTHSPL'
			  AND rel.REL = 'RO'
			""";

	// MTHSPL DP + GUIDs
	private static final String SQL_MTHSPL_DPS = """
			SELECT conso.AUI AS AUI, conso.CUI AS CUI, conso.TTY AS TTY, conso.STR AS STR,
			       sat.ATV AS ATV, sat.CODE AS CODE
			FROM umls.MRCONSO conso, umls.MRSAT sat
			WHERE conso.AUI = sat.METAUI
			  AND conso.TTY = 'DP'
			  AND sat.SUPPRESS = 'N'
			""";

	// SNOMED PT by CUIs (batched)
	private static final String SQL_SNOMED_PT_BY_CUIS_IN = """
			SELECT AUI, CUI, CODE, STR
			FROM umls.MRCONSO
			WHERE SAB = 'SNOMEDCT_US' AND TTY = 'PT' AND SUPPRESS = 'N' AND CUI IN (%s)
			""";

	// SNOMED parents by PT AUIs (batched)
	private static final String SQL_MRHIER_PARENTS_BY_AUIS_IN = """
			SELECT DISTINCT AUI, PAUI
			FROM umls.MRHIER
			WHERE AUI IN (%s)
			""";

	// SNOMED ingredients via DP AUIs (batched)
	private static final String SQL_SNOMED_ING_BY_DP_AUIS_IN = """
			SELECT DISTINCT rel.AUI2 AS DP_AUI, rel.CUI1 AS CUI, rel.AUI1 AS AUI,
			                conso.TTY, conso.STR, conso.CODE
			FROM umls.MRREL rel
			JOIN umls.MRCONSO conso ON rel.AUI1 = conso.AUI
			WHERE conso.SAB = 'SNOMEDCT_US'
			  AND conso.ISPREF = 'Y'
			  AND conso.SUPPRESS = 'N'
			  AND rel.RELA IN ('has_precise_active_ingredient','has_active_ingredient','has_basis_of_strength_substance')
			  AND rel.AUI2 IN (%s)
			""";

	// SNOMED ingredients via PT AUIs (batched)
	private static final String SQL_SNOMED_ING_BY_PT_AUIS_IN = """
			SELECT DISTINCT rel.AUI2 AS AUI2, rel.AUI1 AS AUI, conso.CODE, conso.STR, conso.CUI, conso.TTY
			FROM umls.MRREL rel
			JOIN umls.MRCONSO conso ON rel.AUI1 = conso.AUI
			WHERE rel.SAB = 'SNOMEDCT_US'
			  AND rel.RELA IN ('has_precise_active_ingredient','has_active_ingredient','has_basis_of_strength_substance')
			  AND rel.AUI2 IN (%s)
			""";

	// RxNorm via SPL_SET_ID
	private static final String SQL_RXNORM_BY_SPL_SET_ID = """
			SELECT conso.CUI, conso.AUI, conso.TTY, conso.STR, conso.CODE, sat.ATV
			FROM umls.MRCONSO conso, umls.MRSAT sat
			WHERE sat.ATN = 'SPL_SET_ID'
			  AND sat.CUI = conso.CUI
			  AND conso.SAB = 'RXNORM'
			  AND conso.TTY NOT IN ('TMSY','ET')
			  AND sat.SUPPRESS = 'N'
			""";

	// MedDRA
	private static final String SQL_MEDDRA_ATOMS = """
			SELECT AUI, CUI, TTY, STR, SDUI AS PT_CODE, CODE, SAB
			FROM MRCONSO
			WHERE SAB = 'MDR' AND SUPPRESS = 'N'
			""";

	// RxNorm atoms (batched AUI IN)
	private static final String SQL_RXNORM_ATOMS_BY_AUIS_IN = """
			SELECT AUI, CUI, TTY, STR, CODE
			FROM MRCONSO
			WHERE SAB = 'RXNORM' AND AUI IN (%s)
			""";

	// SNOMED atoms (batched AUI IN)
	private static final String SQL_SNOMED_ATOMS_BY_AUIS_IN = """
			SELECT AUI, CUI, TTY, STR, CODE
			FROM MRCONSO
			WHERE SAB = 'SNOMEDCT_US' AND SUPPRESS <> 'Y' AND AUI IN (%s)
			""";

	// MRSTY (semantic types)
	private static final String SQL_SEMANTIC_TYPES_FOR_MDR = """
			SELECT DISTINCT sty.CUI, sty.STN
			FROM MRSTY sty, MRCONSO conso
			WHERE sty.CUI = conso.CUI AND conso.SAB = 'MDR'
			""";

	private static final int BATCH_SIZE_IN = 1000; // for IN (...) batching

	// =========================================================================
	// Core data stores
	// =========================================================================

	private final Map<String, Atom> rxnormAtoms = new HashMap<>();
	private final Map<String, Map<String, Boolean>> rxnormCuis = new HashMap<>();

	private final Map<String, Atom> snomedAtoms = new HashMap<>();
	private final Map<String, Atom> meddraAtoms = new HashMap<>();
	private final Map<String, Atom> atcAtoms = new HashMap<>();

	private final Map<String, List<String>> meddraCodes = new HashMap<>();
	private final Map<String, Map<String, Boolean>> meddraCuis = new HashMap<>();

	// MTHSPL dictionaries
	private Map<String, HashMap<String, Atom>> mthsplAtoms = new HashMap<>();
	private Map<String, List<String>> dpAuiToSplGuid = new HashMap<>();
	private Map<String, Set<String>> splCuis = new HashMap<>();
	private Set<String> uniqueSplCuis = new HashSet<>();
	private Map<String, List<Atom>> snomedCuiToPT = new HashMap<>();

	private final Map<String, List<String>> atcCodes = new HashMap<>();
	private final Map<String, Map<String, Boolean>> atcCuis = new HashMap<>();

	private final Map<String, Map<String, Boolean>> ndcToAtc = new HashMap<>();
	private final Map<String, Set<String>> ndcToUmlsCui = new HashMap<>();

	// Map brand name to ingredients
	private final Map<String, Map<String, Boolean>> bnToRxNorm = new HashMap<>();
	private final Map<String, Map<String, Boolean>> rxnormToBn = new HashMap<>();
	private final Map<String, Map<String, Boolean>> guidTorxnorm = new HashMap<>();

	// MedDRA terms: TTY -> (cleaned term -> AUIs)
	private final Map<String, Map<String, List<String>>> meddraTerms = new HashMap<>();

	private final Map<String, ArrayList<String>> exactMeddraTermToAuis = new HashMap<>();

	private int MAX_TOKEN_MATCH_LENGTH = -1;
	private final TokenizerModel tokenizerModel;
	private final ThreadLocal<TokenizerME> tokenizerLocal;
	private final ThreadLocal<englishStemmer> stemmerLocal = ThreadLocal.withInitial(englishStemmer::new);

	// TTY -> tokenCount -> (transformedTerm -> AUIs)
	private Map<String, Map<Integer, Map<String, List<String>>>> transformedMeddraMap = new HashMap<>();
	private Map<String, Map<Integer, Map<String, List<String>>>> stemmedTransformedMeddraMap = new HashMap<>();

	// =========================================================================
	// Public API
	// =========================================================================

	public List<String> getValidTty() {
		return VALID_TTY;
	}

	public Map<String, Map<Integer, Map<String, List<String>>>> getTransformedMap() {
		if (transformedMeddraMap == null || transformedMeddraMap.isEmpty()) {
			loadTransformedMeddraMap(false);
		}
		return transformedMeddraMap;
	}

	public Map<String, Map<Integer, Map<String, List<String>>>> getStemmedMap() {
		if (stemmedTransformedMeddraMap == null || stemmedTransformedMeddraMap.isEmpty()) {
			loadStemmedTransformedMeddraMap(false);
		}
		return stemmedTransformedMeddraMap;
	}

	public int getMaxTokenMatchLength() {
		return MAX_TOKEN_MATCH_LENGTH;
	}

	public Map<String, Map<String, List<String>>> getMeddraTerms() {
		return meddraTerms;
	}

	public Map<String, Atom> getMedDRA() {
		return meddraAtoms;
	}

	public Map<String, Atom> getRxNorm() {
		return rxnormAtoms;
	}

	public Map<String, Atom> getSnomed() {
		return snomedAtoms;
	}

	public Map<String, Atom> getAtc() {
		return atcAtoms;
	}

	/**
	 * Find brand name links to an RxNorm entry
	 * 
	 * @param rxnormAui
	 * @return
	 */
	public List<Atom> getBrandNameMatches(String rxnormAui) {
		List<Atom> results = new ArrayList<>();
		if (this.rxnormToBn.containsKey(rxnormAui)) {
			for (String aui : this.rxnormToBn.get(rxnormAui).keySet()) {
				if (this.rxnormAtoms.containsKey(aui)) {
					results.add(this.rxnormAtoms.get(aui));
				}
			}
		}
		return results;
	}

	public List<Atom> getMeddraCuiSynonyms(String cui) {
		return meddraCuis.getOrDefault(cui, Collections.emptyMap()).keySet().stream().map(meddraAtoms::get)
				.collect(Collectors.toList());
	}

	public Atom getMeddraPtCode(String ptcode) {
		return meddraCodes.getOrDefault(ptcode, Collections.emptyList()).stream().map(meddraAtoms::get)
				.filter(a -> "PT".equals(a.getTty())).findFirst().orElse(null);
	}

	// TEST SUPPORT: seed MedDRA terms directly
	void _testPutMeddraTerm(String tty, String term, String aui) {
		Map<String, List<String>> ttyMap = meddraTerms.computeIfAbsent(tty, k -> new HashMap<>());
		ttyMap.computeIfAbsent(term, k -> new ArrayList<>()).add(aui);
	}

	// TEST SUPPORT: seed ATC and NDC->ATC
	void _testPutAtcAtom(String aui, Atom atom) {
		atcAtoms.put(aui, atom);
	}

	void _testLinkNdcToAtc(String ndc, String atcAui) {
		ndcToAtc.computeIfAbsent(ndc, k -> new HashMap<>()).put(atcAui, true);
	}

	// =========================================================================
	// Tokenizer / IO
	// =========================================================================
	private TokenizerModel loadTokenizerModel() {
		try (InputStream in = tryOpen(TOKEN_MODEL_PATH)) {
			if (in == null)
				return null;
			return new TokenizerModel(in);
		} catch (Exception e) {
			return null;
		}
	}

	// === Lazy model loading helpers ===
	private static InputStream tryOpen(String path) {
		// FS first
		try {
			java.nio.file.Path p = java.nio.file.Paths.get(path);
			if (java.nio.file.Files.exists(p))
				return new FileInputStream(p.toFile());
		} catch (Exception ignore) {
		}
		// CLASSPATH
		try {
			InputStream in = UmlsLoader.class.getClassLoader().getResourceAsStream(path);
			if (in != null)
				return in;
		} catch (Exception ignore) {
		}
		return null;
	}

	private void loadRxNorm() {
		Logger.log("Loading RxNorm from UMLS...");
		Db.withConnection(conn -> {
			try (PreparedStatement stmt = conn.prepareStatement(SQL_RXNORM_ATOMS); ResultSet rs = stmt.executeQuery()) {
				while (rs.next()) {
					String aui = rs.getString("AUI");
					String cui = rs.getString("CUI");
					String tty = rs.getString("TTY");
					String term = rs.getString("STR");
					String code = rs.getString("CODE");

					Atom atom = new Atom(aui, cui, null, code, term, tty);
					rxnormAtoms.put(aui, atom);
					rxnormCuis.computeIfAbsent(cui, k -> new HashMap<>()).put(aui, true);
				}
			} catch (SQLException e) {
				Logger.error("Error loading RXNORM: " + e.getMessage(), e);
			}
			return null;
		});
	}

	private void loadBrandNames() {

		Logger.log("Loading brand name relations from UMLS...");
		Db.withConnection(conn -> {
			try (PreparedStatement stmt = conn.prepareStatement(SQL_BN_TO_RXCUI); ResultSet rs = stmt.executeQuery()) {
				while (rs.next()) {

					// Brand name data
					String aui = rs.getString("brand_aui");
//					String cui = rs.getString("brand_cui");
//					String term = rs.getString("brand_name");
//					String code = rs.getString("brand_code");

					// RxNorm code
					String rxAui = rs.getString("linked_aui");
//					String rxCui = rs.getString("linked_cui");
//					String rxTerm = rs.getString("linked_name");
//					String rxCode = rs.getString("linked_code");
//					String rxTty = rs.getString("linked_tty");

					// All of RxNorm has previously been loaded

					// Store the relation
					bnToRxNorm.computeIfAbsent(aui, k -> new HashMap<>()).put(rxAui, true);
					rxnormToBn.computeIfAbsent(rxAui, k -> new HashMap<>()).put(aui, true);

				}
			} catch (SQLException e) {
				Logger.error("Error loading brand names: " + e.getMessage(), e);
			}
			return null;
		});

	}

	// =========================================================================
	// ATC + NDC->ATC
	// =========================================================================
	private void loadATC() {
		Logger.log("Loading ATC codes from UMLS...");
		Db.withConnection(conn -> {
			try (PreparedStatement stmt = conn.prepareStatement(SQL_ATC_ATOMS); ResultSet rs = stmt.executeQuery()) {
				while (rs.next()) {
					String aui = rs.getString("AUI");
					String cui = rs.getString("CUI");
					String tty = rs.getString("TTY");
					String term = rs.getString("STR");
					String code = rs.getString("CODE");

					Atom atom = new Atom(aui, cui, null, code, term, tty);
					atcAtoms.put(aui, atom);
					atcCodes.computeIfAbsent(code, k -> new ArrayList<>()).add(aui);
					atcCuis.computeIfAbsent(cui, k -> new HashMap<>()).put(aui, true);
				}
			} catch (SQLException e) {
				Logger.error("Error loading ATC codes: " + e.getMessage(), e);
			}
			return null;
		});

		Logger.log("Loading all NDC codes from UMLS... 1.2 million");
		Db.withConnection(conn -> {
			try (PreparedStatement stmt = conn.prepareStatement(SQL_NDC_TO_CUI); ResultSet rs = stmt.executeQuery()) {
				while (rs.next()) {
					String cui = rs.getString("CUI");
					String code = rs.getString("ATV");
					ndcToUmlsCui.computeIfAbsent(code, k -> new HashSet<>()).add(cui);
				}
			} catch (SQLException e) {
				Logger.error("Error loading NDC codes: " + e.getMessage(), e);
			}
			return null;
		});

		Logger.log("Loading RxNorm NDC/CUI set to support ATC mapping...");
		Map<String, Map<String, String>> rxCuiToNdc = new HashMap<>();
		Db.withConnection(conn -> {
			try (PreparedStatement stmt = conn.prepareStatement(SQL_RXCUI_TO_NDC); ResultSet rs = stmt.executeQuery()) {
				while (rs.next()) {
					String cui = rs.getString("RX_CUI");
					String ndcCode = rs.getString("NDC");
					String term = rs.getString("RX_STR");
					if (ndcCode != null && ndcCode.length() >= 2) {
						ndcCode = ndcCode.substring(0, ndcCode.length() - 2);
						rxCuiToNdc.computeIfAbsent(cui, k -> new HashMap<>()).put(ndcCode, term);
					}
				}
			} catch (SQLException e) {
				Logger.error("Error loading RxNorm NDC/CUI support: " + e.getMessage(), e);
			}
			return null;
		});

		Logger.log("Linking RxNorm -> ATC via MTHSPL relations...");
		Db.withConnection(conn -> {
			try (PreparedStatement stmt = conn.prepareStatement(SQL_MTHSPL_REL_RO);
					ResultSet rs = stmt.executeQuery()) {
				int counter = 0;
				while (rs.next()) {
					counter++;
					if (counter % 200000 == 0)
						Logger.log(" >> " + counter);

					String cui1 = rs.getString("CUI1");
					String cui2 = rs.getString("CUI2");

					if (rxCuiToNdc.containsKey(cui1) && atcCuis.containsKey(cui2)) {
						Map<String, String> ndcs = rxCuiToNdc.get(cui1);
						for (Map.Entry<String, String> ndcEntry : ndcs.entrySet()) {
							String ndc = ndcEntry.getKey();
							String ndcTermLower = nz(ndcEntry.getValue()).toLowerCase(ROOT);
							for (String atcAui : atcCuis.get(cui2).keySet()) {
								Atom atc = atcAtoms.get(atcAui);
								String atcLower = nz(atc.getTerm()).toLowerCase(ROOT);
								if (!atcLower.isEmpty() && ndcTermLower.contains(atcLower)) {
									ndcToAtc.computeIfAbsent(ndc, k -> new HashMap<>()).put(atcAui, true);
								}
							}
						}
					}
				}
			} catch (SQLException e) {
				Logger.error("Error loading RxNorm -> ATC relations: " + e.getMessage(), e);
			}
			return null;
		});

		Logger.log("Total NDC -> ATC maps: " + ndcToAtc.size());
	}

	// =========================================================================
	// MTHSPL GUID mapping (enrichment with SNOMED + RxNorm)
	// =========================================================================
	/**
	 * Map SPL GUIDs (set ids) to UMLS drug products; enrich with SNOMED
	 * parents/ingredients and RxNorm attributes. Uses batched/parameterized SQL.
	 */
	public ConcurrentLinkedQueue<SplDrug> getMappedGuid(Map<String, List<String>> guidToXml) {

		Logger.log("Getting mapped GUID products");

		// If an SPL is not found in the UMLS, it will not be mapped unless we can
		// link it via NDC code or otherwise to a mapped GUID
		ConcurrentLinkedQueue<SplDrug> finalProducts = new ConcurrentLinkedQueue<SplDrug>();
		Map<String, SplDrug> mappedSpls = new HashMap<>();
		Map<String, SplDrug> unmappedSpls = new HashMap<>();

		if (guidToXml == null || guidToXml.isEmpty())
			return finalProducts;

		if (GUID_MAP_LOADED == false)
			preloadGuidMapSupport();

		try {
			// Step 1:
			Logger.log("Attaching MTHSPL GUID maps...");
			for (String guid : guidToXml.keySet()) {
				// Defaults for a new drug assigned to this GUID
				SplDrug drug = new SplDrug();
				drug.setGuid(guid);
				drug.setSave(false);

				for (String xml : guidToXml.get(guid)) {
					// Update drug source type based on XML files
					if (xml.contains("prescription") && drug.getSourceType() == -1)
						drug.setSourceType(1);

					if (xml.contains("other") && drug.getSourceType() == -1)
						drug.setSourceType(2);

					if (xml.contains("otc") && drug.getSourceType() == -1)
						drug.setSourceType(2);

					drug.addXmlFile(xml, false);
				}

				if (mthsplAtoms.containsKey(guid)) {
					HashMap<String, Atom> atoms = mthsplAtoms.get(guid);
					for (String aui : atoms.keySet()) {
						Atom atom = atoms.get(aui);
						drug.getDrugProduct().put(aui, atom);
						drug.addDrugProductCui(atom.getCui());
					}
					drug.setSave(true);

					// Product was found in UMLS
					mappedSpls.put(guid, drug);
				} else {
					unmappedSpls.put(guid, drug);
				}
			}

			// Step 2: Attach SNOMED PTs to SPL DPs, build PT AUI set for later
			Logger.log("Mapping SNOMED PTs to SPL...");
			Map<String, Boolean> snomedPtAuis = new HashMap<>();
			for (Map.Entry<String, SplDrug> e : mappedSpls.entrySet()) {
				String guid = e.getKey();
				SplDrug drug = e.getValue();
				for (String cui : drug.getDrugProductCuis()) {
					List<Atom> pts = snomedCuiToPT.get(cui);
					if (pts == null)
						continue;
					for (Atom pt : pts) {
						drug.addSnomedPt(pt);
						dpAuiToSplGuid.computeIfAbsent(pt.getAui(), k -> new ArrayList<>()).add(guid);
						snomedPtAuis.put(pt.getAui(), true);
					}
				}
			}

			// --- 3) SNOMED parents for PT AUIs (batched by AUI)
			Logger.log("Adding SNOMED parent concepts (batched)...");
			Map<String, List<String>> snomedParents = new HashMap<>();
			List<String> ptAuis = new ArrayList<>(snomedPtAuis.keySet());
			Db.forBatches(ptAuis, BATCH_SIZE_IN, batch -> {
				String sql = Db.bindIn(SQL_MRHIER_PARENTS_BY_AUIS_IN, batch.size());
				Db.withConnection(conn -> {
					try (PreparedStatement ps = conn.prepareStatement(sql)) {
						Db.bind(ps, batch);
						try (ResultSet rs = ps.executeQuery()) {
							while (rs.next()) {
								String aui = rs.getString("AUI");
								String paui = rs.getString("PAUI");
								snomedParents.computeIfAbsent(aui, k -> new ArrayList<>()).add(paui);
							}
						}
					} catch (SQLException e1) {
						Logger.error("Error loading SNOMED parent concepts: " + e1.getMessage(), e1);
					}
					return null;
				});
			});

			// --- 4) SNOMED ingredients via DP AUI (batched)
			Logger.log("Adding active ingredients via MTHSPL DP (batched)...");
			List<String> dpAuis = new ArrayList<>(dpAuiToSplGuid.keySet());
			Db.forBatches(dpAuis, BATCH_SIZE_IN, batch -> {
				String sql = Db.bindIn(SQL_SNOMED_ING_BY_DP_AUIS_IN, batch.size());
				Db.withConnection(conn -> {
					try (PreparedStatement ps = conn.prepareStatement(sql)) {
						Db.bind(ps, batch);
						try (ResultSet rs = ps.executeQuery()) {
							while (rs.next()) {
								String dpAui = rs.getString("DP_AUI");
								List<String> guids = dpAuiToSplGuid.get(dpAui);
								if (guids == null)
									continue;

								Atom ing = new Atom();
								ing.setAui(rs.getString("AUI"));
								ing.setCui(rs.getString("CUI"));
								ing.setTty(rs.getString("TTY"));
								ing.setCode(rs.getString("CODE"));
								ing.setTerm(rs.getString("STR"));
								ing.setSab("SNOMEDCT_US");

								for (String guid : guids) {
									SplDrug drg = mappedSpls.get(guid);
									if (drg != null)
										drg.addIngedient(ing);
								}
							}
						}
					} catch (SQLException e1) {
						Logger.error("Error loading active ingredients: " + e1.getMessage(), e1);
					}
					return null;
				});
			});

			// --- 5) SNOMED ingredients via PT AUI (batched)
			Logger.log("Adding active ingredients via SNOMED PT link (batched)...");
			Map<String, Map<String, Atom>> snomedPtActIng = new HashMap<>();
			Db.forBatches(ptAuis, BATCH_SIZE_IN, batch -> {
				String sql = Db.bindIn(SQL_SNOMED_ING_BY_PT_AUIS_IN, batch.size());
				Db.withConnection(conn -> {
					try (PreparedStatement ps = conn.prepareStatement(sql)) {
						Db.bind(ps, batch);
						try (ResultSet rs = ps.executeQuery()) {
							while (rs.next()) {
								String testAui = rs.getString("AUI2");
								Map<String, Atom> perPt = snomedPtActIng.computeIfAbsent(testAui, k -> new HashMap<>());
								Atom ing = new Atom();
								ing.setAui(rs.getString("AUI"));
								ing.setCui(rs.getString("CUI"));
								ing.setTty(rs.getString("TTY"));
								ing.setCode(rs.getString("CODE"));
								ing.setTerm(rs.getString("STR"));
								ing.setSab("SNOMEDCT_US");
								perPt.put(ing.getAui(), ing);
							}
						}
					} catch (SQLException e1) {
						Logger.error("Error loading active ingredients via SNOMED PT Link: " + e1.getMessage(), e1);
					}
					return null;
				});
			});

			// Attach PT-linked ingredients & parents
			for (Map.Entry<String, SplDrug> e : mappedSpls.entrySet()) {
				SplDrug drg = e.getValue();
				for (String ptAui : drg.getSnomedPts().keySet()) {
					List<String> parents = snomedParents.get(ptAui);
					if (parents != null)
						parents.forEach(drg::addSnomedParent);

					Map<String, Atom> ings = snomedPtActIng.get(ptAui);
					if (ings != null)
						ings.values().forEach(drg::addIngedient);
				}
			}

			// --- 6) Add any RxNorm link found via the SPL_SET_ID
			Logger.log("RxNorm enrichment via SPL_SET_ID maps...");
			for (String guid : mappedSpls.keySet()) {
				if (guidTorxnorm.containsKey(guid)) {
					SplDrug drg = mappedSpls.get(guid);
					for (String aui : guidTorxnorm.get(guid).keySet()) {
						Atom rxAtom = rxnormAtoms.get(aui);
						drg.addRxNormPt(rxAtom);
					}
				}
			}

			// --- 7) Extract NDC codes from the label and assign to all substances
			Logger.log("Extract NDC codes from SPLs");
			extractAndAssignNdcsParallel(guidToXml, mappedSpls, unmappedSpls, 0);

			// Note: At this step, the XML files have not been processed.
			// Therefore, we are updating the SplDrug to retains it's GUID and
			// XML files for processing - it will copy over the mapped CUIs from
			// the original substance that did map.
			Logger.log("Begin merge missing GUID files");
			MergeResult ndcMerge = mergeMissingGuidFilesByNdc(mappedSpls, unmappedSpls, 0);

			Logger.log("Merged missing GUID XMLs on NDC: " + ndcMerge.merged.size());
			Map<String, SplDrug> stillUnmapped = ndcMerge.stillUnmapped;

			// Last step, try to find NDC codes in any RxNorm entry
			// to assign to this SPL - this should help us capture
			// discontinued produts that no longer have an MTH_SPL set entry
			List<SplDrug> finalNdcMatchList = new ArrayList<>();
			for (String guid : stillUnmapped.keySet()) {

				SplDrug drug = stillUnmapped.get(guid);
				Set<String> ndcs = drug.getRawNdcCodes();
				Set<String> candidateCuiSet = new HashSet<>();

				// Test all NDC codes for any potential CUI match via RxNorm
				for (String ndc : ndcs) {
					if (ndcToUmlsCui.containsKey(ndc)) {
						candidateCuiSet.addAll(ndcToUmlsCui.get(ndc));
					}
				}

				// Search for RxNorm links
				boolean hasRxNormLink = false;
				for (String cui : candidateCuiSet) {
					if (rxnormCuis.containsKey(cui)) {
						for (String aui : rxnormCuis.get(cui).keySet()) {
							if (rxnormAtoms.containsKey(aui)) {
								Atom entry = rxnormAtoms.get(aui);
								drug.addDrugProductCui(cui);
								drug.addRxNormPt(entry);
								drug.setSave(true);
								hasRxNormLink = true;

							}
						}
					}
				}

				if (hasRxNormLink) {
					finalNdcMatchList.add(drug);
				}

			}
			Logger.log("Found additional NDC to RxNorm links:  " + finalNdcMatchList.size());

			Set<String> seenGuid = new HashSet<>();

			// Create final list of products to process
			for (SplDrug drg : mappedSpls.values()) {
				if (drg.getGuid() != null && seenGuid.contains(drg.getGuid()) == false) {
					finalProducts.add(drg);
					seenGuid.add(drg.getGuid());
				} else {
					Logger.error("Attemp [1] to store GUID more than once: " + drg.getGuid());
				}
			}

			for (SplDrug drg : ndcMerge.merged) {
				if (drg.getGuid() != null && seenGuid.contains(drg.getGuid()) == false) {
					finalProducts.add(drg);
					seenGuid.add(drg.getGuid());
				} else {
					Logger.error("Attemp [2] to store GUID more than once: " + drg.getGuid());
				}
			}

			for (SplDrug drg : finalNdcMatchList) {
				if (drg.getGuid() != null && seenGuid.contains(drg.getGuid()) == false) {
					finalProducts.add(drg);
					seenGuid.add(drg.getGuid());
				} else {
					Logger.error("Attemp [3] to store GUID more than once: " + drg.getGuid());
				}
			}

			return finalProducts;
		} catch (

		Exception e) {
			Logger.log("Error getting MTHSPL GUID maps: " + e);
			return null;
		}
	}

	/**
	 * Pad NDC codes with leading zeros if they are not the full length
	 * 
	 * @param ndcCode
	 * @return
	 */
	private String padNdcCode(String ndcCode) {
		if (ndcCode.startsWith("0")) {
			while (ndcCode.length() < EXAMPLE_NDC.length()) {
				ndcCode = "0" + ndcCode;
			}
		}
		return ndcCode;
	}

	private boolean findSplNdcMatchAndUpdate(Map<String, SplDrug> mappedSpls, SplDrug drg, String ndcCode) {

		boolean isMerged = false;
		for (SplDrug drg2 : mappedSpls.values()) {
			// Test for inclusion via NDC codes
			if (isMerged == false && drg2.getRawNdcCodes().contains(ndcCode)) {
				// Copy the product links to this one
				drg.getDrugProductCuis().addAll(drg2.getDrugProductCuis());
				for (String aui : drg2.getIngredients().keySet())
					drg.addIngedient(drg2.getIngredients().get(aui));

				drg.getSnomedParentAuis().addAll(drg2.getSnomedParentAuis());
				for (String aui : drg2.getSnomedPts().keySet())
					drg.addSnomedPt(drg2.getSnomedPts().get(aui));

				// Set to save
				drg.setSave(true);
				return true;
			}
		}
		return false;
	}

	private void preloadGuidMapSupport() {
		if (GUID_MAP_LOADED == false) {

			Logger.log("Loading MTHSPL GUIDs...");
			Db.withConnection(conn -> {
				try (PreparedStatement stmt = conn.prepareStatement(SQL_MTHSPL_DPS);
						ResultSet rs = stmt.executeQuery()) {
					while (rs.next()) {
						String guid = rs.getString("ATV");
						String aui = rs.getString("AUI");
						String cui = rs.getString("CUI");
						String tty = rs.getString("TTY");
						String str = rs.getString("STR");
						String code = rs.getString("CODE");

						Atom atom = new Atom();
						atom.setAui(aui);
						atom.setCui(cui);
						atom.setTerm(str);
						atom.setTty(tty);
						atom.setSab("MTHSPL");
						atom.setCode(code);

						// Store for global lookup
						if (mthsplAtoms.containsKey(guid)) {
							mthsplAtoms.get(guid).put(aui, atom);
							splCuis.get(guid).add(cui);
							uniqueSplCuis.add(cui);
						} else {
							mthsplAtoms.put(guid, new HashMap<String, Atom>());
							mthsplAtoms.get(guid).put(aui, atom);
							splCuis.put(guid, new HashSet<String>());
							splCuis.get(guid).add(cui);
							uniqueSplCuis.add(cui);
						}
					}
				} catch (SQLException e1) {
					Logger.error("Error loading MTHSPL GUIDs: " + e1.getMessage(), e1);
				}
				return null;
			});

			Logger.log("Total UMLS MTHSPL GUIDs: " + mthsplAtoms.size());

			Logger.log("Loading SNOMED PT by CUI (batched)...");
			List<String> cuiSet = new ArrayList<String>();
			for (String cui : uniqueSplCuis)
				cuiSet.add(cui);

			Db.forBatches(cuiSet, BATCH_SIZE_IN, batch -> {
				String sql = Db.bindIn(SQL_SNOMED_PT_BY_CUIS_IN, batch.size());
				Db.withConnection(conn -> {
					try (PreparedStatement ps = conn.prepareStatement(sql)) {
						Db.bind(ps, batch);
						try (ResultSet rs = ps.executeQuery()) {
							while (rs.next()) {
								String cui = rs.getString("CUI");
								String aui = rs.getString("AUI");
								String code = rs.getString("CODE");
								String term = rs.getString("STR");

								Atom atom = new Atom();
								atom.setAui(aui);
								atom.setCui(cui);
								atom.setTerm(term);
								atom.setCode(code);
								atom.setSab("SNOMEDCT_US");
								atom.setTty("PT");

								snomedCuiToPT.computeIfAbsent(cui, k -> new ArrayList<>()).add(atom);
							}
						}
					} catch (SQLException e1) {
						Logger.error("Error loading SNOMED PT CUIs: " + e1.getMessage(), e1);
					}
					return null;
				});
			});

			Logger.log("Loading RxNorm to MTH links...");
			Db.withConnection(conn -> {
				try (PreparedStatement stmt = conn.prepareStatement(SQL_RXNORM_BY_SPL_SET_ID);
						ResultSet rs = stmt.executeQuery()) {
					while (rs.next()) {
						String guid = rs.getString("ATV");
						String aui = rs.getString("AUI");
						String cui = rs.getString("CUI");
						String tty = rs.getString("TTY");
						String str = rs.getString("STR");
						String code = rs.getString("CODE");

						// Store the atom for later lookup
						Atom atom = new Atom();
						atom.setAui(aui);
						atom.setCui(cui);
						atom.setTerm(str);
						atom.setCode(code);
						atom.setSab("RXNORM");
						atom.setTty(tty);
						rxnormAtoms.put(aui, atom);
						rxnormCuis.computeIfAbsent(cui, k -> new HashMap<>()).put(aui, true);

						// Link GUID to RxNorm
						if (guidTorxnorm.containsKey(guid) == false) {
							guidTorxnorm.put(guid, new HashMap<>());
						}
						guidTorxnorm.get(guid).put(aui, true);
					}
				} catch (SQLException e1) {
					Logger.error("Error loading RxNorm to MTH links: " + e1.getMessage(), e1);
				}
				return null;
			});

			// Flag that this has been loaded
			GUID_MAP_LOADED = true;
		}
	}

	// =========================================================================
	// NDC lookup
	// =========================================================================
	public static List<String> getDbNdcCodes() {
		List<String> ndcCodes = new ArrayList<>();
		Db.withConnection(conn -> {
			Logger.log("Loading NDC codes from DB...");
			try (PreparedStatement stmt = conn.prepareStatement("SELECT NDC_CODE FROM NDC_CODE");
					ResultSet rs = stmt.executeQuery()) {
				while (rs.next())
					ndcCodes.add(rs.getString("NDC_CODE"));
			} catch (SQLException e) {
				Logger.error("Error loading NDC codes from DB: " + e.getMessage(), e);
			}
			return null;
		});
		return ndcCodes;
	}

	/**
	 * Returns all ATC atoms linked to a given NDC code (using the prebuilt ndcToAtc
	 * index). The list is non-null; order is deterministic by AUI.
	 *
	 * @param ndcCode 11-digit NDC (without the last two check digits if you
	 *                followed the trimming step).
	 * @return list of ATC {@link Atom} records linked to the NDC code.
	 */
	public List<Atom> getNdcToAtcCodes(String ndcCode) {
		if (ndcCode == null)
			return Collections.emptyList();
		Map<String, Boolean> auis = this.ndcToAtc.get(ndcCode);
		if (auis == null || auis.isEmpty())
			return Collections.emptyList();

		// Deterministic order by AUI
		List<String> sortedAuis = new ArrayList<>(auis.keySet());
		Collections.sort(sortedAuis);

		List<Atom> results = new ArrayList<>(sortedAuis.size());
		for (String aui : sortedAuis) {
			Atom atom = atcAtoms.get(aui);
			if (atom != null)
				results.add(atom);
		}
		return results;
	}

	/**
	 * Extract NDC codes from SPL XMLs in parallel (with progress logging), then
	 * merge into SplDrug objects and add padded variants. This method is
	 * thread-safe with respect to NdcExtractor and does all SplDrug mutations after
	 * the parallel stage.
	 *
	 * @param guidToXml    Map: GUID -> list of XML file paths belonging to that
	 *                     GUID
	 * @param mappedSpls   Map of GUID -> SplDrug (mapped)
	 * @param unmappedSpls Map of GUID -> SplDrug (unmapped)
	 * @param maxThreads   Desired parallelism (e.g., 4..8). If <= 0, computed from
	 *                     CPUs.
	 */
	private void extractAndAssignNdcsParallel(Map<String, List<String>> guidToXml, Map<String, SplDrug> mappedSpls,
			Map<String, SplDrug> unmappedSpls, int maxThreads) {
		// ----- Config / pool -----
		int numProcs = Runtime.getRuntime().availableProcessors();
		final int parallelism = (maxThreads > 0) ? maxThreads : Math.max(2, numProcs - 2);

		// Flatten to (guid, xmlFile) pairs
		final List<AbstractMap.SimpleEntry<String, String>> tasks = new ArrayList<>();
		for (Map.Entry<String, List<String>> e : guidToXml.entrySet()) {
			final String g = e.getKey();
			final List<String> files = e.getValue();
			for (String f : files) {
				tasks.add(new AbstractMap.SimpleEntry<>(g, f));
			}
		}

		final int totalGuids = guidToXml.size();
		final long totalXml = tasks.size(); // <-- make it final & derived from tasks

		// Progress counters
		final AtomicLong xmlDone = new AtomicLong(0);
		final AtomicInteger guidsTouched = new AtomicInteger(0);
		final Set<String> seenGuidForTouch = ConcurrentHashMap.newKeySet();

		// Logging cadence (can safely use final totalXml)
		final long XML_LOG_EVERY = Math.max(100L, totalXml / 200); // ~0.5% or min 100
		final long LOG_HEARTBEAT_MS = 5000L;
		final AtomicLong lastLogMs = new AtomicLong(System.currentTimeMillis());

		Logger.log(
				String.format("NDC extraction (XML-level): starting with %,d GUIDs and %,d XML files (parallelism=%d)",
						totalGuids, totalXml, parallelism));

		final NdcExtractor ndcExtractor = new NdcExtractor();

		// Concurrent result: GUID -> concurrent set of NDCs
		final ConcurrentHashMap<String, Set<String>> guidToNdcs = new ConcurrentHashMap<>();

		// Dedicated pool (don’t rely on common pool)
		final ForkJoinPool pool = new ForkJoinPool(parallelism);
		try {
			pool.submit(() -> tasks.parallelStream().unordered().forEach(pair -> {
				final String guid = pair.getKey();
				final String xmlFile = pair.getValue();
				try {
					final List<String> found = ndcExtractor.getNdcCodes(xmlFile);
					if (found != null && !found.isEmpty()) {
						// Get/create concurrent set for this GUID
						final Set<String> ndcSet = guidToNdcs.computeIfAbsent(guid, k -> ConcurrentHashMap.newKeySet());
						ndcSet.addAll(found);
					}
				} catch (Exception ex) {
					Logger.log("Skipping malformed SPL XML: " + xmlFile + " (" + ex.getClass().getSimpleName() + ")");
				} finally {
					long done = xmlDone.incrementAndGet();
					if (seenGuidForTouch.add(guid)) {
						guidsTouched.incrementAndGet();
					}
					long now = System.currentTimeMillis();
					if (done % XML_LOG_EVERY == 0 || (now - lastLogMs.get()) >= LOG_HEARTBEAT_MS) {
						lastLogMs.set(now);
						double pctXml = (totalXml == 0) ? 100.0 : (100.0 * done / totalXml);
						Logger.log(String.format("NDC extraction: XML %,d / %,d (%.1f%%) | GUIDs touched: %,d / %,d",
								done, totalXml, pctXml, guidsTouched.get(), totalGuids));
					}
				}
			})).get(); // wait for completion
		} catch (InterruptedException ie) {
			Thread.currentThread().interrupt();
			throw new RuntimeException("NDC extraction interrupted", ie);
		} catch (ExecutionException ee) {
			throw new RuntimeException("NDC extraction failed", ee.getCause());
		} finally {
			pool.shutdown();
		}

		// ----- Serial merge: mutate SplDrug safely + add padded forms -----
		for (Map.Entry<String, Set<String>> e : guidToNdcs.entrySet()) {
			final String guid = e.getKey();
			final Set<String> ndcs = e.getValue();
			if (ndcs == null || ndcs.isEmpty())
				continue;

			final SplDrug drg = mappedSpls.containsKey(guid) ? mappedSpls.get(guid) : unmappedSpls.get(guid);
			if (drg == null)
				continue;

			drg.getRawNdcCodes().addAll(ndcs);
			for (String ndcCode : ndcs) {
				final String padded = padNdcCode(ndcCode);
				if (padded != null && !padded.isEmpty()) {
					drg.getRawNdcCodes().add(padded);
				}
			}
		}

		Logger.log(String.format("NDC extraction: complete. GUIDs with NDCs: %,d", guidToNdcs.size()));
	}

	// =========================================================================
	// Transform maps (parallel builds)
	// =========================================================================

	private Map<String, Map<Integer, Map<String, List<String>>>> loadTransformedMeddraMap(boolean restrictTty) {
		if (this.transformedMeddraMap != null && !this.transformedMeddraMap.isEmpty()) {
			return this.transformedMeddraMap;
		}
		Logger.log("Creating transformed MedDRA map (parallel)...");
		Map<String, Map<Integer, Map<String, List<String>>>> result = new ConcurrentHashMap<>();
		int[] maxTok = { 0 };

		Collection<String> ttys = restrictTty ? VALID_TTY : this.meddraTerms.keySet();

		ttys.parallelStream().forEach(tty -> {
			Map<String, List<String>> ttyTermMap = this.meddraTerms.get(tty);
			if (ttyTermMap == null)
				return;

			Map<Integer, Map<String, List<String>>> transformedByTok = new ConcurrentHashMap<>();
			ttyTermMap.entrySet().parallelStream().forEach(e -> {
				String verbatim = e.getKey();
				List<String> auis = e.getValue();

				String raw = (verbatim == null ? "" : verbatim);
				String lower = normalizePunct(raw).toLowerCase(ROOT);

				// tokenize using OpenNLP (or the test tokenizer), then join possessives
				String[] toks = (_testTokenizer != null) ? _testTokenizer.apply(lower)
						: tokenizerLocal.get().tokenize(lower);
				List<String> tokList = joinPossessives(toks);

				int tokenCount = tokList.size();
				String normalizedKey = String.join(" ", tokList);

				// keep MAX_TOKEN_MATCH_LENGTH accurate
				if (tokenCount > MAX_TOKEN_MATCH_LENGTH)
					MAX_TOKEN_MATCH_LENGTH = tokenCount;

				// base key
				transformedByTok.computeIfAbsent(tokenCount, k -> new ConcurrentHashMap<>()).put(normalizedKey, auis);

				// qualifier-first variant
				String rotated = qualifierFirstVariant(normalizedKey);
				if (rotated != null) {
					transformedByTok.computeIfAbsent(tokenCount, k -> new ConcurrentHashMap<>()).put(rotated, auis);
				}

				// "of the" -> adjective rotation (e.g., "carcinoma of the ovary" -> "ovarian
				// carcinoma")
				String ofThe = ofTheRotation(normalizedKey);
				if (ofThe != null) {
					transformedByTok.computeIfAbsent(tokenCount, k -> new ConcurrentHashMap<>()).put(ofThe, auis);
				}

			});
			result.put(tty, transformedByTok);
		});

		this.MAX_TOKEN_MATCH_LENGTH = maxTok[0];
		this.transformedMeddraMap = Collections.unmodifiableMap(result);
		return this.transformedMeddraMap;
	}

	private Map<String, Map<Integer, Map<String, List<String>>>> loadStemmedTransformedMeddraMap(boolean restrictTty) {
		if (this.stemmedTransformedMeddraMap != null && !this.stemmedTransformedMeddraMap.isEmpty()) {
			return this.stemmedTransformedMeddraMap;
		}
		Logger.log("Creating stemmed transformed MedDRA map (optimized with progress)...");

		Map<String, Map<Integer, Map<String, List<String>>>> result = new ConcurrentHashMap<>();
		Collection<String> ttys = restrictTty ? VALID_TTY : this.meddraTerms.keySet();

		// ---- Progress counters ----
		final java.util.concurrent.atomic.LongAdder processed = new java.util.concurrent.atomic.LongAdder();
		final long total = ttys.stream().map(this.meddraTerms::get).filter(Objects::nonNull).mapToLong(Map::size).sum();
		final long startMs = System.currentTimeMillis();
		final java.util.concurrent.atomic.AtomicLong nextLogAtMs = new java.util.concurrent.atomic.AtomicLong(startMs);
		final java.util.concurrent.atomic.AtomicInteger nextPct = new java.util.concurrent.atomic.AtomicInteger(
				PROGRESS_STEP_PERCENT);

		// track max safely without contended writes
		final java.util.concurrent.atomic.AtomicInteger localMaxTok = new java.util.concurrent.atomic.AtomicInteger(1);

		// Outer loop: TTYs are few; keep sequential. Inner: terms parallel.
		for (String tty : ttys) {
			Map<String, List<String>> ttyTermMap = this.meddraTerms.get(tty);
			if (ttyTermMap == null || ttyTermMap.isEmpty())
				continue;

			Map<Integer, Map<String, List<String>>> transformedByTok = new ConcurrentHashMap<>();

			// Parallelize across many terms in this TTY
			ttyTermMap.entrySet().parallelStream().forEach(e -> {
				String verbatim = e.getKey();
				List<String> auis = e.getValue();

				String raw = (verbatim == null ? "" : verbatim);
				String lower = normalizePunct(raw).toLowerCase(ROOT);

				// tokenize once
				String[] toks = (_testTokenizer != null) ? _testTokenizer.apply(lower)
						: tokenizerLocal.get().tokenize(lower);
				List<String> tokList = joinPossessives(toks);
				int tokenCount = tokList.size();

				// update local max
				localMaxTok.getAndUpdate(prev -> Math.max(prev, tokenCount));

				// stem once → stems[]
				englishStemmer stemmer = stemmerLocal.get();
				String[] stems = new String[tokList.size()];
				for (int i = 0; i < tokList.size(); i++) {
					String t = tokList.get(i);
					stemmer.setCurrent(t);
					stems[i] = stemmer.stem() ? stemmer.getCurrent() : t;
				}

				// base key (stemmed)
				String baseStem = joinTokens(stems, 0, stems.length);

				// rotations computed on ORIGINAL tokens (order logic), then mapped
				String origJoined = String.join(" ", tokList);

				// qualifier-first rotation (same token count)
				String qfStem = null;
				String qf = qualifierFirstVariant(origJoined);
				if (qf != null && stems.length >= 3) {
					qfStem = joinTokens(stems, stems.length - 2, stems.length) + ' '
							+ joinTokens(stems, 0, stems.length - 2);
				}

				// "of the" rotation (rare): stem the rotated small array
				String ofTheStem = null;
				String ofThe = ofTheRotation(origJoined);
				if (ofThe != null) {
					String[] parts = ofThe.split("\\s+");
					String[] partsStem = new String[parts.length];
					for (int i = 0; i < parts.length; i++) {
						String pt = parts[i];
						stemmer.setCurrent(pt);
						partsStem[i] = stemmer.stem() ? stemmer.getCurrent() : pt;
					}
					ofTheStem = joinTokens(partsStem, 0, partsStem.length);
				}

				// insert variants
				Map<String, List<String>> bucket = transformedByTok.computeIfAbsent(tokenCount,
						k -> new ConcurrentHashMap<>());
				bucket.put(baseStem, auis);
				if (qfStem != null)
					bucket.put(qfStem, auis);
				if (ofTheStem != null)
					bucket.put(ofTheStem, auis);

				// ---- progress bump & conditional log ----
				if (PROGRESS_ENABLED) {
					processed.increment();
					maybeLogProgress(processed.longValue(), total, startMs, nextLogAtMs, nextPct);
				}
			});

			result.put(tty, transformedByTok);
		}

		// publish max safely once
		int maxNow = localMaxTok.get();
		if (maxNow > MAX_TOKEN_MATCH_LENGTH) {
			MAX_TOKEN_MATCH_LENGTH = maxNow;
		}

		// Final progress line
		if (PROGRESS_ENABLED) {
			long elapsed = System.currentTimeMillis() - startMs;
			Logger.log(String.format("Stemmed map build complete: %,d / %,d (100%%) in %s", total, total,
					formatDuration(elapsed)));
		}

		this.stemmedTransformedMeddraMap = Collections.unmodifiableMap(result);
		return this.stemmedTransformedMeddraMap;
	}

	// Log at most every N% and no more frequently than PROGRESS_MIN_INTERVAL_MS
	private void maybeLogProgress(long done, long total, long startMs,
			java.util.concurrent.atomic.AtomicLong nextLogAtMs, java.util.concurrent.atomic.AtomicInteger nextPct) {
		if (total <= 0L)
			return;
		final long now = System.currentTimeMillis();

		// percent done (1..99)
		int pct = (int) Math.min(99, Math.max(1, Math.floorDiv(done * 100L, Math.max(1L, total))));
		int targetPct = nextPct.get();

		// Only if we crossed the next % step and passed the time interval
		if (pct >= targetPct && now >= nextLogAtMs.get()) {
			// best-effort advance guards (no strict synchronization needed)
			nextPct.compareAndSet(targetPct, Math.min(99, targetPct + PROGRESS_STEP_PERCENT));
			nextLogAtMs.compareAndSet(nextLogAtMs.get(), now + PROGRESS_MIN_INTERVAL_MS);

			long elapsed = now - startMs;
			long estTotalMs = (done == 0) ? 0 : (elapsed * total) / done;
			long eta = Math.max(0, estTotalMs - elapsed);

			Logger.log(String.format("Building stemmed MedDRA map: %,d / %,d (%d%%), elapsed %s, ETA %s", done, total,
					pct, formatDuration(elapsed), formatDuration(eta)));
		}
	}

	// --- tiny helpers ---
	private static String joinTokens(String[] arr, int start, int end) {
		if (start >= end)
			return "";
		int n = end - start;
		StringBuilder sb = new StringBuilder(n * 6);
		for (int i = start; i < end; i++) {
			if (i > start)
				sb.append(' ');
			sb.append(arr[i]);
		}
		return sb.toString();
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

	// =========================================================================
	// MedDRA loading & pruning
	// =========================================================================

	private void loadMedDRA() {
		Map<String, Boolean> ignoredAuiBySemanticType = getIgnoredAuisBySemanticType();
		Logger.log("Ignore Semantic Type AUIs: " + ignoredAuiBySemanticType.size());

		Logger.log("Loading MedDRA from UMLS...");
		Db.withConnection(conn -> {
			try (PreparedStatement stmt = conn.prepareStatement(SQL_MEDDRA_ATOMS); ResultSet rs = stmt.executeQuery()) {
				while (rs.next()) {
					String aui = rs.getString("AUI");
					String tty = rs.getString("TTY");

					if (!VALID_TTY.contains(tty) || ignoredAuiBySemanticType.containsKey(aui)) {
						continue;
					}

					String term = rs.getString("STR");
					if (stopword.isStopword(term))
						continue;

					String cui = rs.getString("CUI");
					String ptCode = rs.getString("PT_CODE");
					String code = rs.getString("CODE");

					Atom atom = new Atom(aui, cui, ptCode, code, term, tty);
					meddraAtoms.put(aui, atom);
					meddraCodes.computeIfAbsent(code, k -> new ArrayList<>()).add(aui);
					meddraCuis.computeIfAbsent(cui, k -> new HashMap<>()).put(aui, true);

					exactMeddraTermToAuis.computeIfAbsent(term, k -> new ArrayList<>()).add(aui);

					String cleanedTerm = umlsCleanText(term).trim();
					String reversedTerm = reverseTerm(cleanedTerm);

					Map<String, List<String>> ttyMap = meddraTerms.computeIfAbsent(tty, k -> new HashMap<>());
					ttyMap.computeIfAbsent(cleanedTerm, k -> new ArrayList<>()).add(aui);
					if (!reversedTerm.equals(cleanedTerm)) {
						ttyMap.computeIfAbsent(reversedTerm, k -> new ArrayList<>()).add(aui);
					}
				}
			} catch (SQLException e) {
				Logger.error("Error loading MedDRA: " + e.getMessage(), e);
			}
			return null;
		});

		removeDuplicateLLTAtoms();
	}

	private void removeDuplicateLLTAtoms() {
		Set<String> toRemove = new HashSet<>();

		for (Map.Entry<String, Atom> e : meddraAtoms.entrySet()) {
			Atom atom = e.getValue();
			if (!"LLT".equals(atom.getTty()))
				continue;

			List<String> auiList = meddraCodes.get(atom.getCode());
			if (auiList == null)
				continue;

			for (String aui : auiList) {
				Atom pt = meddraAtoms.get(aui);
				if (pt != null && "PT".equals(pt.getTty()) && nz(pt.getTerm()).equals(nz(atom.getTerm()))) {
					toRemove.add(e.getKey());
					break;
				}
			}
		}

		for (String aui : toRemove) {
			Atom atom = meddraAtoms.remove(aui);
			if (atom == null)
				continue;

			List<String> list = meddraCodes.get(atom.getCode());
			if (list != null) {
				list.remove(aui);
				if (list.isEmpty())
					meddraCodes.remove(atom.getCode());
			}

			Map<String, Boolean> cuiMap = meddraCuis.get(atom.getCui());
			if (cuiMap != null)
				cuiMap.remove(aui);

			String cleaned = umlsCleanText(nz(atom.getTerm()).toLowerCase(ROOT)).trim();
			for (String tty : VALID_TTY) {
				Map<String, List<String>> ttyMap = meddraTerms.get(tty);
				if (ttyMap == null)
					continue;
				List<String> ids = ttyMap.get(cleaned);
				if (ids != null)
					ids.remove(aui);
			}
		}
	}

	// =========================================================================
	// RxNorm & SNOMED subset loaders (batched AUI IN)
	// =========================================================================

	public void loadRxNorm(ConcurrentLinkedQueue<SplDrug> allProducts) {

		// AUIs referenced by products
		Set<String> needed = new HashSet<>();
		for (SplDrug drg : allProducts)
			needed.addAll(drg.getRxNormPts().keySet());

		Logger.log("Loading RxNorm from UMLS (batched)...");
		Logger.log(" >> Total AUIs to load: " + needed.size());

		if (needed.isEmpty())
			return;

		Db.forBatches(new ArrayList<>(needed), BATCH_SIZE_IN, batch -> {
			String sql = Db.bindIn(SQL_RXNORM_ATOMS_BY_AUIS_IN, batch.size());
			Db.withConnection(conn -> {
				try (PreparedStatement ps = conn.prepareStatement(sql)) {
					Db.bind(ps, batch);
					try (ResultSet rs = ps.executeQuery()) {
						while (rs.next()) {
							String aui = rs.getString("AUI");
							String cui = rs.getString("CUI");
							String tty = rs.getString("TTY");
							String code = rs.getString("CODE");
							String term = nz(rs.getString("STR")).toLowerCase(ROOT).trim().replace("\"", "'");

							rxnormAtoms.put(aui, new Atom(aui, cui, null, code, term, tty));
							rxnormCuis.computeIfAbsent(cui, k -> new HashMap<>()).put(aui, true);
						}
					}
				} catch (SQLException e) {
					e.printStackTrace();
				}
				return null;
			});
		});

		Logger.log(" >> Total atoms loaded: " + rxnormAtoms.size());
		long missing = needed.stream().filter(a -> !rxnormAtoms.containsKey(a)).count();
		Logger.log(" >> Total missing: " + missing);
	}

	public void loadSnomed(ConcurrentLinkedQueue<SplDrug> allProducts) {
		Set<String> needed = new HashSet<>();
		for (SplDrug drg : allProducts) {
			needed.addAll(drg.getIngredients().keySet());
			needed.addAll(drg.getSnomedParentAuis());
			needed.addAll(drg.getSnomedPts().keySet());
		}

		Logger.log("Creating SNOMED subset table from UMLS (batched)...");
		Logger.log(" >> Total AUIs to load: " + needed.size());
		if (needed.isEmpty())
			return;

		Db.forBatches(new ArrayList<>(needed), BATCH_SIZE_IN, batch -> {
			String sql = Db.bindIn(SQL_SNOMED_ATOMS_BY_AUIS_IN, batch.size());
			Db.withConnection(conn -> {
				try (PreparedStatement ps = conn.prepareStatement(sql)) {
					Db.bind(ps, batch);
					try (ResultSet rs = ps.executeQuery()) {
						while (rs.next()) {
							String aui = rs.getString("AUI");
							String cui = rs.getString("CUI");
							String tty = rs.getString("TTY");
							String code = rs.getString("CODE");
							String term = nz(rs.getString("STR")).toLowerCase(ROOT).trim().replace("\"", "'");

							snomedAtoms.put(aui, new Atom(aui, cui, null, code, term, tty));
						}
					}
				} catch (SQLException e) {
					e.printStackTrace();
				}
				return null;
			});
		});

		Logger.log(" >> Total atoms generated: " + snomedAtoms.size());
		long missing = needed.stream().filter(a -> !snomedAtoms.containsKey(a)).count();
		Logger.log(" >> Total missing: " + missing);
	}

	// =========================================================================
	// Semantic types
	// =========================================================================

	public static Map<String, Set<String>> loadSemanticTypes() {
		Logger.log("Loading Semantic Types from UMLS...");
		Map<String, Set<String>> cuiToTypes = new HashMap<>();
		Db.withConnection(conn -> {
			try (PreparedStatement stmt = conn.prepareStatement(SQL_SEMANTIC_TYPES_FOR_MDR);
					ResultSet rs = stmt.executeQuery()) {
				while (rs.next()) {
					String cui = rs.getString("CUI");
					String stn = rs.getString("STN");
					for (String inc : INCLUDED_SEMANTIC_TYPES) {
						if (stn != null && stn.contains(inc)) {
							cuiToTypes.computeIfAbsent(cui, k -> new HashSet<>()).add(inc);
							break;
						}
					}
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			return null;
		});
		return cuiToTypes;
	}

	public Map<String, Boolean> getIgnoredAuisBySemanticType() {
		Logger.log("Finding MedDRA concepts to ignore based on semantic types...");
		Map<String, Boolean> ignore = new HashMap<>();

		Map<String, Set<String>> cuiToTypes;
		try {
			cuiToTypes = loadSemanticTypes();
		} catch (Exception e) {
			Logger.log("Error loading semantic types: " + e);
			return ignore;
		}

		Db.withConnection(conn -> {
			String query = "SELECT AUI, CUI FROM MRCONSO WHERE SAB='MDR'";
			try (PreparedStatement stmt = conn.prepareStatement(query); ResultSet rs = stmt.executeQuery()) {
				while (rs.next()) {
					String aui = rs.getString("AUI");
					String cui = rs.getString("CUI");
					Set<String> types = cuiToTypes.get(cui);
					if (types == null || Collections.disjoint(types, INCLUDED_SEMANTIC_TYPES)) {
						ignore.put(aui, true);
					}
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			return null;
		});
		return ignore;
	}

	// =========================================================================
	// Text helpers
	// =========================================================================

	/**
	 * Cleans text for string comparison by removing punctuation and normalizing
	 * case.
	 */
	public static String umlsCleanText(String oldString) {
		String term = nz(oldString).replaceAll("[^a-zA-Z0-9\\s-]", " ").toLowerCase(ROOT).trim();
		if (term.contains("\""))
			term = term.replace("\"", "'");
		return term;
	}

	/**
	 * Heuristic: swap leading/trailing modifiers like increase/decrease/abnormal.
	 */
	public static String reverseTerm(String input) {
		if (StringUtils.isBlank(input))
			return input;
		String[] keywords = { "increased", "increase", "decreased", "decrease", "abnormal" };
		for (String k : keywords) {
			if (input.endsWith(k)) {
				String prefix = input.substring(0, input.length() - k.length()).trim();
				return (k + " " + prefix).trim();
			}
			if (input.startsWith(k)) {
				String suffix = input.substring(k.length()).trim();
				return (suffix + " " + k).trim();
			}
		}
		return input;
	}

	private static String nz(String s) {
		return (s == null) ? "" : s;
	}

	// Helper functions
	private static String canonicalizeNdc(String ndc) {
		if (ndc == null)
			return "";
		// If you already normalize to 5-4-2 with padNdcCode, you can use that then
		// strip non-digits.
		// String padded = padNdcCode(ndc);
		// return (padded == null) ? "" : padded.replaceAll("[^0-9]", "");
		return ndc.replaceAll("[^0-9]", "");
	}

	private static String qualifierFirstVariant(String normalizedLower) {
		if (normalizedLower == null)
			return null;
		normalizedLower = normalizedLower.trim();
		if (normalizedLower.isEmpty())
			return null;

		String[] parts = normalizedLower.split("\\s+");
		int n = parts.length;
		if (n < 3)
			return null;

		String t1 = parts[n - 2]; // candidate qualifier: aids/hiv/radiation/drug
		String t2 = parts[n - 1]; // candidate tail: related / induced (any prefix OK: relat*, induc*)

		// normalizedLower is already lower-cased by the caller
		// Expandable list of qualifiers
		java.util.Set<String> qualifiers = java.util.Set.of("aids", "hiv", "radiation", "drug");

		boolean isQualifierTail = (t2.startsWith("relat") || t2.startsWith("induc"));
		boolean isMatch = qualifiers.contains(t1) && isQualifierTail;

		if (!isMatch)
			return null;

		// Rotate: "<...> t1 t2" -> "t1 t2 <...>"
		StringBuilder sb = new StringBuilder(normalizedLower.length());
		sb.append(t1).append(' ').append(t2).append(' ');
		for (int i = 0; i < n - 2; i++) {
			if (i > 0)
				sb.append(' ');
			sb.append(parts[i]);
		}
		return sb.toString();
	}

	private static String ofTheRotation(String key) {
		var m = java.util.regex.Pattern.compile("^(\\p{L}+) of the (\\p{L}+)\\b").matcher(key);
		if (m.find()) {
			String head = m.group(1); // carcinoma
			String organ = m.group(2); // ovary
			String adj = ORGAN_ADJ.getOrDefault(organ, organ);
			return (adj + " " + head).trim();
		}
		return null;
	}

	private static String normalizePunct(String s) {
		if (s == null)
			return "";
		String out = s.replace('\u2019', '\'').replace('\u2018', '\'').replace('\u201C', '"').replace('\u201D', '"')
				.replace('\u2013', '-').replace('\u2014', '-').replace('\u2212', '-');
		// AIDS-related -> aids related ; Kaposi’s -> Kaposi's
		out = out.replaceAll("\\s*'\\s*s\\b", "'s");
		out = out.replaceAll("(?<=\\p{L})-(?=\\p{L})", " "); // split hyphenated words
		out = out.replaceAll("\\s+(['-])\\s+", "$1");
		return out;
	}

	private static List<String> joinPossessives(String[] toks) {
		ArrayList<String> out = new ArrayList<>(toks.length);
		for (int i = 0; i < toks.length; i++) {
			String t = toks[i];
			if ("'s".equalsIgnoreCase(t) && !out.isEmpty()) {
				out.set(out.size() - 1, out.get(out.size() - 1) + "'s");
			} else if ("'".equals(t) && i + 1 < toks.length && "s".equalsIgnoreCase(toks[i + 1])) {
				if (!out.isEmpty())
					out.set(out.size() - 1, out.get(out.size() - 1) + "'s");
				i++; // skip the "s"
			} else {
				out.add(t);
			}
		}
		return out;
	}

	// Simple result carrier
	private static final class MergeResult {
		final List<SplDrug> merged; // drugs that found a mapped partner via NDC
		final Map<String, SplDrug> stillUnmapped; // the rest

		MergeResult(List<SplDrug> merged, Map<String, SplDrug> stillUnmapped) {
			this.merged = merged;
			this.stillUnmapped = stillUnmapped;
		}
	}

	/**
	 * Merge "unmapped" SPL drugs to mapped ones by matching NDC codes. - Builds an
	 * index: canonical NDC -> candidate mapped SplDrug(s) - Walks unmapped drugs in
	 * parallel - For each drug, stops after the first successful
	 * findSplNdcMatchAndUpdate
	 *
	 * Thread-safety: only 'drg' (the unmapped target) is mutated inside the
	 * parallel block. 'mappedSpls' is read-only here. 'findSplNdcMatchAndUpdate'
	 * must not mutate shared state other than the passed 'drg'.
	 *
	 * @param mappedSpls   GUID -> mapped SplDrug
	 * @param unmappedSpls GUID -> unmapped SplDrug
	 * @param maxThreads   desired parallelism (<=0 to auto-compute)
	 */
	private MergeResult mergeMissingGuidFilesByNdc(Map<String, SplDrug> mappedSpls, Map<String, SplDrug> unmappedSpls,
			int maxThreads) {
		Logger.log("Begin merge missing GUID files (indexed + parallel)");

		// --- 1) Build NDC index from mapped SPLs: canonical NDC -> candidates
		Map<String, List<SplDrug>> ndcIndex = new HashMap<>(Math.max(16384, mappedSpls.size()));
		for (SplDrug m : mappedSpls.values()) {
			for (String ndc : m.getRawNdcCodes()) {
				String key = canonicalizeNdc(ndc);
				if (!key.isEmpty()) {
					ndcIndex.computeIfAbsent(key, k -> new ArrayList<>(1)).add(m);
				}
			}
		}

		// --- 2) Parallel pass over UNMAPPED
		int numProcs = Runtime.getRuntime().availableProcessors();
		final int parallelism = (maxThreads > 0) ? maxThreads : Math.max(2, numProcs - 2);

		final java.util.concurrent.ConcurrentLinkedQueue<SplDrug> merged = new java.util.concurrent.ConcurrentLinkedQueue<>();
		final java.util.concurrent.ConcurrentHashMap<String, SplDrug> stillUnmapped = new java.util.concurrent.ConcurrentHashMap<>();

		java.util.concurrent.ForkJoinPool pool = new java.util.concurrent.ForkJoinPool(parallelism);
		try {
			pool.submit(() -> unmappedSpls.entrySet().parallelStream().forEach(e -> {
				final String guid = e.getKey();
				final SplDrug drg = e.getValue();
				if (guid == null || guid.isEmpty() || drg == null)
					return;

				boolean isMerged = false;
				// avoid checking the same canonical NDC more than once for this drug
				java.util.HashSet<String> seen = new java.util.HashSet<>(drg.getRawNdcCodes().size() * 2);

				for (String ndcCode : drg.getRawNdcCodes()) {
					String key = canonicalizeNdc(ndcCode);
					if (key.isEmpty() || !seen.add(key))
						continue;

					List<SplDrug> candidates = ndcIndex.get(key);
					if (candidates == null)
						continue;

					// For each ndcCode…
					if (findSplNdcMatchAndUpdate(mappedSpls, drg, ndcCode)) {
					    isMerged = true;
					    break; // stop scanning NDCs once merged
					}
					
					if (isMerged)
						break;
				}

				if (isMerged)
					merged.add(drg);
				else
					stillUnmapped.put(guid, drg);
			})).get();
		} catch (InterruptedException ie) {
			Thread.currentThread().interrupt();
			throw new RuntimeException("NDC merge interrupted", ie);
		} catch (java.util.concurrent.ExecutionException ee) {
			throw new RuntimeException("NDC merge failed", ee.getCause());
		} finally {
			pool.shutdown();
		}

		Logger.log("Merged missing GUID XMLs on NDC (parallel): " + merged.size());
		return new MergeResult(new ArrayList<>(merged), stillUnmapped);
	}

	// =========================================================================
	// Tiny DB helper with retries & batching
	// =========================================================================

	/**
	 * Minimal DB helper: centralizes connection building with retries and provides
	 * small utilities for IN (...) batching.
	 */
	private static final class Db {
		private static final int MAX_ATTEMPTS = 3;
		private static final Duration BASE_BACKOFF = Duration.ofMillis(250);

		/** Obtain a Connection with retries/backoff. */
		static Connection getConnection() throws SQLException {
			// Optionally: Class.forName(DB_DRIVER);
			String url = "jdbc:mysql://" + DB_HOST + ":" + DB_PORT + "/" + UMLS_DB_NAME + "?user=" + DB_USER
					+ "&password=" + DB_PASS;
			SQLException last = null;
			for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
				try {
					return DriverManager.getConnection(url);
				} catch (SQLException e) {
					last = e;
					if (attempt == MAX_ATTEMPTS)
						break;
					backoff(attempt);
				}
			}
			throw last;
		}

		/** Run a unit of work with a Connection (auto-close), with retries. */
		static <T> T withConnection(Function<Connection, T> work) {
			for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
				try (Connection conn = getConnection()) {
					return work.apply(conn);
				} catch (SQLException e) {
					if (attempt == MAX_ATTEMPTS) {
						Logger.log("DB error (giving up): " + e);
						break;
					} else {
						Logger.log("DB error (retrying): " + e);
						backoff(attempt);
					}
				}
			}
			return null;
		}

		private static void backoff(int attempt) {
			long jitter = ThreadLocalRandom.current().nextLong(50, 150);
			long sleepMs = BASE_BACKOFF.toMillis() * (1L << (attempt - 1)) + jitter;
			try {
				Thread.sleep(sleepMs);
			} catch (InterruptedException ie) {
				Thread.currentThread().interrupt();
			}
		}

		/** Create "..., ?, ?, ?" placeholders for IN clauses. */
		static String bindIn(String sqlTemplate, int n) {
			if (n <= 0)
				throw new IllegalArgumentException("n must be > 0");
			String placeholders = String.join(",", Collections.nCopies(n, "?"));
			return String.format(sqlTemplate, placeholders);
		}

		/** Bind a list of values (String) to a PreparedStatement IN (...) */
		static void bind(PreparedStatement ps, List<String> values) throws SQLException {
			int idx = 1;
			for (String v : values)
				ps.setString(idx++, v);
		}

		/** Batch helper: iterate a collection in fixed-size chunks. */
		static <T> void forBatches(List<T> items, int batchSize, java.util.function.Consumer<List<T>> consumer) {
			for (int i = 0; i < items.size(); i += batchSize) {
				int end = Math.min(i + batchSize, items.size());
				consumer.accept(items.subList(i, end));
			}
		}
	}
}
