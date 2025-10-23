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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import org.pvlens.spl.umls.UmlsLoader;
import org.tartarus.snowball.ext.EnglishStemmer;

import opennlp.tools.doccat.DoccatModel;
import opennlp.tools.doccat.DocumentCategorizerME;
import opennlp.tools.lemmatizer.DictionaryLemmatizer;
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;

/**
 * MedDRA term extraction from SPL text with section gating, exclusions,
 * stopwording, (optional) lemmatization/stemming, and longest-first matching.
 */
public class MedDRAProcessor {

	/** Sections parsed by this processor. */
	public enum AeSection { IND, AE, BLACKBOX }

	/** Composite absorption rules: if composite present, drop its component. */
	private static final Map<String, String> COMPOSITE_ABSORPTION = new HashMap<>();
	static {
		// consolidate common oncology phrasing
		COMPOSITE_ABSORPTION.put("aids related kaposi's sarcoma", "aids");
		COMPOSITE_ABSORPTION.put("kaposi's sarcoma", "sarcoma");
	}

	// === Feature Flags (default ON as previously) ===
	private static final boolean ENABLE_LEMMA = true;
	private static final boolean ENABLE_DOCCAT = true;

	// Doccat acceptance threshold
	private static final double DOCCAT_THRESHOLD = 0.25;

	// Do not gate BLACKBOX with doccat (ALWAYS evaluate black box warnings)
	private static final boolean DOCCAT_GATE_BLACKBOX = false;

	// model file paths
	private static final String POS_MODEL_PATH = "models/en-pos-maxent.bin";
	private static final String LEMMA_DICT_PATH = "models/en-lemmatizer.dict";
	private static final String DOCCAT_MODEL_PATH = "models/meddra-section-doccat.bin";

	// Shared (singleton) OpenNLP models — loaded once
	private static final AtomicReference<POSModel> POS_MODEL_REF = new AtomicReference<>();
	private static final AtomicReference<TokenizerModel> TOKEN_MODEL_REF = new AtomicReference<>();
	private static final AtomicReference<DoccatModel> DOCCAT_MODEL_REF = new AtomicReference<>();
	private static final AtomicReference<DictionaryLemmatizer> LEMMATIZER_REF = new AtomicReference<>();

	// === Lazy model loading helpers ===
	private static InputStream tryOpen(String path) {
		// FS first
		try {
			java.nio.file.Path p = java.nio.file.Paths.get(path);
			if (java.nio.file.Files.exists(p)) return new FileInputStream(p.toFile());
		} catch (Exception ignore) {}
		// Classpath next
		try {
			InputStream in = MedDRAProcessor.class.getClassLoader().getResourceAsStream(path);
			if (in != null) return in;
		} catch (Exception ignore) {}
		return null;
	}

	private static POSModel getPosModel() {
		POSModel m = POS_MODEL_REF.get();
		if (m != null) return m;
		try (InputStream in = tryOpen(POS_MODEL_PATH)) {
			if (in == null) return null;
			POSModel nm = new POSModel(in);
			POS_MODEL_REF.compareAndSet(null, nm);
			return POS_MODEL_REF.get();
		} catch (Exception e) { return null; }
	}

	private static TokenizerModel getTokModel() {
		TokenizerModel m = TOKEN_MODEL_REF.get();
		if (m != null) return m;
		try (InputStream in = tryOpen(System.getProperty("pvlens.meddra.tokModel", "models/en-token.bin"))) {
			if (in == null) return null;
			TokenizerModel nm = new TokenizerModel(in);
			TOKEN_MODEL_REF.compareAndSet(null, nm);
			return TOKEN_MODEL_REF.get();
		} catch (Exception e) { return null; }
	}

	private static DictionaryLemmatizer getLemmatizer() {
		DictionaryLemmatizer l = LEMMATIZER_REF.get();
		if (l != null) return l;
		try (InputStream in = tryOpen(LEMMA_DICT_PATH)) {
			if (in == null) return null;
			DictionaryLemmatizer nl = new DictionaryLemmatizer(in);
			LEMMATIZER_REF.compareAndSet(null, nl);
			return LEMMATIZER_REF.get();
		} catch (Exception e) { return null; }
	}

	private static DoccatModel getDoccatModel() {
		DoccatModel m = DOCCAT_MODEL_REF.get();
		if (m != null) return m;
		try (InputStream in = tryOpen(DOCCAT_MODEL_PATH)) {
			if (in == null) return null;
			DoccatModel nm = new DoccatModel(in);
			DOCCAT_MODEL_REF.compareAndSet(null, nm);
			return DOCCAT_MODEL_REF.get();
		} catch (Exception e) { return null; }
	}

	// Tokenizer wrapper (shared model)
	private static final ThreadLocal<TokenizerME> TOKENIZER = ThreadLocal.withInitial(() -> {
		TokenizerModel tm = getTokModel();
		return (tm == null) ? null : new TokenizerME(tm);
	});

	// POS tagger wrapper (shared model)
	private static final ThreadLocal<POSTaggerME> POS = ThreadLocal.withInitial(() -> {
		POSModel pm = getPosModel();
		return (pm == null) ? null : new POSTaggerME(pm);
	});

	// SINGLE shared doccat (categorizer can be shared safely)
	private static final AtomicReference<DocumentCategorizerME> DOCCAT = new AtomicReference<>();

	private static DocumentCategorizerME getDoccat() {
		if (!ENABLE_DOCCAT) return null;
		DocumentCategorizerME existing = DOCCAT.get();
		if (existing != null) return existing;
		DoccatModel dm = getDoccatModel();
		if (dm == null) return null;
		DocumentCategorizerME cat = new DocumentCategorizerME(dm);
		DOCCAT.compareAndSet(null, cat);
		return DOCCAT.get();
	}

	// SINGLE shared lemmatizer (thread-safe for reads)
	private static DictionaryLemmatizer sharedLemma() { return getLemmatizer(); }

	// === POS / Lemmatizer ===
	private static final ThreadLocal<EnglishStemmer> STEMMER = ThreadLocal.withInitial(EnglishStemmer::new);

	// ---------------- Configuration & dictionaries ----------------

	private final int MAX_TOKEN_MATCH_LENGTH;
	private final StopwordRemover stopword;
	private final Map<String, Map<Integer, Map<String, List<String>>>> transformedMap;
	private final Map<String, Map<Integer, Map<String, List<String>>>> transformedStemmedMap;
	private final List<String> validTty;

	// ---------------- Exclusion / context patterns ----------------

	/** AE section exclusion patterns. */
	private static final List<Pattern> AE_EXCLUSION_PATTERNS = Arrays.asList(
			Pattern.compile("\\bnot (been )?(associated|observed|reported|recommended)\\b", Pattern.CASE_INSENSITIVE),
			Pattern.compile("\\b(may be|not) ([^ ]+ )?(associated|impaired|demonstrated|shown)\\b", Pattern.CASE_INSENSITIVE),
			Pattern.compile("\\bis suspected\\b", Pattern.CASE_INSENSITIVE),
			Pattern.compile("\\bdisease[- ]related\\b", Pattern.CASE_INSENSITIVE),
			Pattern.compile("\\bfrom causes other\\b", Pattern.CASE_INSENSITIVE),
			Pattern.compile("\\bfrom other causes\\b", Pattern.CASE_INSENSITIVE),
			Pattern.compile("\\bno evidence\\b", Pattern.CASE_INSENSITIVE),
			Pattern.compile("\\bnot (accompanied|indicated|expected|suspected)\\b", Pattern.CASE_INSENSITIVE),
			Pattern.compile("\\bno( significant)? difference\\b", Pattern.CASE_INSENSITIVE),
			Pattern.compile("\\b(single|case) report\\b", Pattern.CASE_INSENSITIVE)
	);

	/** IND section exclusion patterns */
	private static final List<Pattern> IND_EXCLUSION_PATTERNS = Arrays.asList(
			Pattern.compile("\\b(were|is|may be|not) ([^ ]+ )?(associated|observed|reported|impaired|demonstrated|shown)\\b", Pattern.CASE_INSENSITIVE),
			Pattern.compile("\\b(may|might|can|could) occur\\b", Pattern.CASE_INSENSITIVE),
			Pattern.compile("\\bhas caused\\b", Pattern.CASE_INSENSITIVE),
			Pattern.compile("\\bcauses\\b", Pattern.CASE_INSENSITIVE),
			Pattern.compile("\\bincreased incidence\\b", Pattern.CASE_INSENSITIVE),
			Pattern.compile("\\blimitations of use\\b", Pattern.CASE_INSENSITIVE),
			Pattern.compile("\\b(not|was|were)\\s+not\\s+studied\\b", Pattern.CASE_INSENSITIVE),
			Pattern.compile("\\bwas\\s+not\\s+studied\\s+in\\s+patients\\b", Pattern.CASE_INSENSITIVE),
			Pattern.compile("\\bsee (contraind|overdos|abuse|dosage|warnings|precautions|interactions|symptoms and treatment of overdos|(description of)?clinical studies|posology)\\b", Pattern.CASE_INSENSITIVE),
			Pattern.compile("\\b(receptive|insertive)?\\s*(anal|oral|vaginal)\\s+(sex|intercourse|coitus)\\b", Pattern.CASE_INSENSITIVE)			
	);

	/** Study exclusion phrasing (skip entirely in AE/BLACKBOX). */
	private static final List<Pattern> STUDY_EXCLUSION_PATTERNS = Arrays.asList(
			Pattern.compile("\\b(exclusion criteria|were excluded if|patients? (were )?excluded (if|for)|the trial excluded patients? with)\\b", Pattern.CASE_INSENSITIVE),
			Pattern.compile("\\b(ineligible|ineligibility criteria)\\b", Pattern.CASE_INSENSITIVE)
	);

	/** Baseline/history/known-preexisting phrasing (skip in AE/BLACKBOX). */
	private static final List<Pattern> BASELINE_HISTORY_PATTERNS = Arrays.asList(
			Pattern.compile("\\bhistory of\\b", Pattern.CASE_INSENSITIVE),
			Pattern.compile("\\b(known|pre\\-existing|preexisting)\\b", Pattern.CASE_INSENSITIVE),
			Pattern.compile("\\bat baseline\\b", Pattern.CASE_INSENSITIVE),
			Pattern.compile("\\b(baseline|screening) (abnormal|elevated|low|reduced)\\b", Pattern.CASE_INSENSITIVE),
			Pattern.compile("\\bin patients? with\\b", Pattern.CASE_INSENSITIVE)
	);

	/** Local-context negation/attribution substrings (cheap checks). */
	private static final String[] NEGATION_PATTERNS = {
			"not indicated for","not indicative of","no evidence of","not suggestive of","no signs of","does not indicate","without evidence of",
			"free from","absence of","rule out","secondary to","due to an underlying","because of an underlying",
			"resulting from a pre-existing","caused by a pre-existing","have not been","exclusion criteria",
			"were excluded if","patients were excluded","trial excluded patients","history of","at baseline","baseline","pre-existing","preexisting","known"
	};

	private static final List<LabTrigger> LAB_TRIGGERS = List.of(
			new LabTrigger("\\bbilirubin\\b", "\\b(elevated|increase|increased|rise|raised|high)\\b", "blood bilirubin increased"),
			new LabTrigger("\\balkaline\\s+phosphatase\\b", "\\b(elevated|increase|increased|rise|raised|high)\\b", "alkaline phosphatase increased"),
			new LabTrigger("\\b(ast|aspartate\\s+aminotransferase)\\b","\\b(elevated|increase|increased|rise|raised|high)\\b", "aspartate aminotransferase increased")
	);

	/** Dosage-form / product nouns (branding cue after copula). */
	private static final Pattern PRODUCT_CUE_AFTER = Pattern.compile(
			"\\b(is|are|was|were)\\b.{0,48}\\b(" +
					"cream|ointment|gel|lotion|spray|solution|suspension|tablet|capsule|injection|patch|kit|device|" +
					"topical|pre[- ]?radiation|brand|product|dose|dosage|mg|mcg|ml" + ")\\b",
			Pattern.CASE_INSENSITIVE);

	/** Instructional product cues (limited) – defeated by nearby clinical cues. */
	private static final Pattern PRODUCT_INSTRUCTION =
			Pattern.compile("\\b(apply|applied)\\b|\\bfor\\s+external\\s+use\\b", Pattern.CASE_INSENSITIVE);

	/** Clinical cues (allow, unless dosage-form branding is present). */
	private static final Pattern CLINICAL_CUE =
			Pattern.compile("\\b(treat|treatment\\s+of|prevent|prevention\\s+of|manage|management\\s+of|indicated\\s+for)\\b", Pattern.CASE_INSENSITIVE);

	// Lexicon path (classpath or filesystem)
	private static final String ANTONYM_LEXICON_PATH = "lexicon/AM.DB";

	// Bidirectional indices
	private static final Map<String, Set<String>> ANTONYM_INDEX = new HashMap<>();
	private static final Map<String, Set<String>> ANTONYM_INDEX_STEMMED = new HashMap<>();
	private static volatile boolean ANTONYM_LOADED = false;

	// ---------------- Construction ----------------

	public MedDRAProcessor(UmlsLoader umlsLoader) {
		this.stopword = StopwordRemover.getInstance();
		this.transformedMap = umlsLoader.getTransformedMap();
		this.transformedStemmedMap = umlsLoader.getStemmedMap();
		this.validTty = umlsLoader.getValidTty();
		this.MAX_TOKEN_MATCH_LENGTH = umlsLoader.getMaxTokenMatchLength();

		// Load SPECIALIST antonym lexicon once if present
		if (!ANTONYM_LOADED) {
			synchronized (ANTONYM_INDEX) {
				if (!ANTONYM_LOADED) {
					loadAntonymLexicon();
					ANTONYM_LOADED = true;
				}
			}
		}
	}

	// ---------------- Public API ----------------

	/** Backwards-compatible entry point: "IND", "AE", or "BLACKBOX". */
	public Map<String, List<String>> processText(String aeType, String text, boolean exactMatch) {
		AeSection section = toSection(aeType);
		return processText(section, text, exactMatch);
	}

	/**
	 * Extract MedDRA terms from a sentence or short paragraph.
	 * @param section    IND/AE/BLACKBOX
	 * @param text       raw text (may contain multiple sentences)
	 * @param exactMatch if false, applies stopword+stemming and uses stemmed dictionaries
	 */
	public Map<String, List<String>> processText(AeSection section, String text, boolean exactMatch) {
		if (text == null) return Collections.emptyMap();

		// 1) Normalize once
		String normalized = text.toLowerCase(Locale.ROOT).trim();
		if (normalized.isEmpty()) return Collections.emptyMap();
		normalized = normalizePunct(normalized);

		// --- Doccat section gate ---
		DocumentCategorizerME doccat = getDoccat();
		if (doccat != null) {
			boolean shouldGate = (section != AeSection.BLACKBOX) || DOCCAT_GATE_BLACKBOX;
			if (shouldGate) {
				String[] docTokens;
				try {
					TokenizerME tok = TOKENIZER.get();
					docTokens = (tok != null) ? tok.tokenize(normalized) : normalized.split("\\s+");
				} catch (Exception e) {
					docTokens = normalized.split("\\s+");
				}
				double[] probs = doccat.categorize(docTokens);
				String best = doccat.getBestCategory(probs);
				String target = section.toString();

				if (!best.equalsIgnoreCase(target)) {
					int idx = -1;
					for (int i = 0; i < doccat.getNumberOfCategories(); i++) {
						if (doccat.getCategory(i).equalsIgnoreCase(target)) { idx = i; break; }
					}
					double pTarget = (idx >= 0) ? probs[idx] : 0.0;
					if (pTarget < DOCCAT_THRESHOLD) return Collections.emptyMap();
				}
			}
		}

		// 2) Coarse context screen for the section
		if (!isRelevantContext(normalized, section)) return Collections.emptyMap();

		// 3) Sentence-level exclusions on RAW text (pre-stopwords)
		if (isExcludedSentence(section, normalized)) return Collections.emptyMap();

		// 4) Tokenize -> optional stopword/stemming
		TokenizerME tok = TOKENIZER.get();
		String forTok = exactMatch ? normalized : stopword.removeStopwords(normalized);
		String[] tokens = (tok != null) ? tok.tokenize(forTok) : forTok.split("\\s+");

		// Non-exact: use lemmatizer if enabled, else fallback to stemmer
		String[] grams;
		if (exactMatch) {
			grams = joinPossessives(tokens);
		} else {
			if (ENABLE_LEMMA && POS.get() != null && sharedLemma() != null) {
				grams = stemTokens(lemmatizeTokens(tokens));
			} else {
				grams = stemTokens(tokens);
			}
		}

		// 5) Longest-first dictionary match with per-occurrence local gating
		return findMatches(section, grams, exactMatch ? transformedMap : transformedStemmedMap, normalized);
	}

	// ---------------- Core matching ----------------

	private Map<String, List<String>> findMatches(AeSection section, String[] tokens,
			Map<String, Map<Integer, Map<String, List<String>>>> dict, String rawSentence) {

		Map<String, List<String>> out = new HashMap<>();
		if (tokens == null || tokens.length == 0) return out;

		// safety: skip if raw still fails high-level exclusions
		if (isExcludedSentence(section, rawSentence)) return out;

		final StringBuilder key = new StringBuilder(128);
		final int maxN = Math.min(MAX_TOKEN_MATCH_LENGTH, tokens.length);

		// 5) Collect raw matches by key first
		final Set<String> matchedKeys = new HashSet<>();

		for (String tty : orderedTtysFor(section, validTty)) {
			Map<Integer, Map<String, List<String>>> byLen = dict.get(tty);
			if (byLen == null) continue;

			for (int n = maxN; n >= 1; n--) {
				Map<String, List<String>> inner = byLen.get(n);
				if (inner == null) continue;

				for (int i = 0; i <= tokens.length - n; i++) {
					String k = ngramKey(tokens, i, n, key);
					List<String> auis = inner.get(k);
					if (auis == null) continue;
					if (isAllowedByLocalContext(rawSentence, k)) {
						matchedKeys.add(k);
					}
				}
			}
		}

		// 6) Post-processing: antonym guard, composite absorption, drop generics
		suppressAntonyms(matchedKeys, tokens);
		absorbComponents(matchedKeys);
		Set<String> finalKeys = dropGenericWhenSpecific(matchedKeys);

		if (section != AeSection.IND) {
			for (LabTrigger lt : LAB_TRIGGERS) {
				if (lt.analyte.matcher(rawSentence).find() && lt.dir.matcher(rawSentence).find()) {
					for (String tty : dict.keySet()) {
						Map<Integer, Map<String, List<String>>> byLen = dict.get(tty);
						if (byLen == null) continue;
						List<String> candidate = byLen.values().stream().map(m -> m.get(lt.key))
								.filter(Objects::nonNull).findFirst().orElse(null);
						if (candidate != null) {
							finalKeys.add(lt.key);
							break;
						}
					}
				}
			}
		}

		// 7) Rebuild output only for final kept keys
		for (String tty : orderedTtysFor(section, validTty)) {
			Map<Integer, Map<String, List<String>>> byLen = dict.get(tty);
			if (byLen == null) continue;
			for (Map.Entry<Integer, Map<String, List<String>>> e : byLen.entrySet()) {
				Map<String, List<String>> inner = e.getValue();
				for (String k : finalKeys) {
					List<String> auis = inner.get(k);
					if (auis != null) out.putIfAbsent(k, auis);
				}
			}
		}
		return out;
	}

	/** Build a space-joined n-gram key without extra allocations. */
	private static String ngramKey(String[] toks, int start, int len, StringBuilder sb) {
		sb.setLength(0);
		int end = start + len;
		sb.append(toks[start]);
		for (int i = start + 1; i < end; i++) sb.append(' ').append(toks[i]);
		return sb.toString();
	}

	// ---------------- Context & sentence-level filters ----------------

	private static AeSection toSection(String aeType) {
		if (aeType == null) return AeSection.AE;
		switch (aeType.toUpperCase(Locale.ROOT)) {
			case "IND": return AeSection.IND;
			case "BLACKBOX": return AeSection.BLACKBOX;
			case "AE":
			default: return AeSection.AE;
		}
	}

	/** Coarse section context gate. */
	private boolean isRelevantContext(String sentence, AeSection section) {
		switch (section) {
			case IND: return isIndicationContext(sentence);
			case AE:
			case BLACKBOX:
			default: return true;
		}
	}

	/** IND is typically “indicated for”, “treatment of”, “prevention of”, not “in patients with …”. */
	private boolean isIndicationContext(String sentence) {
		if (sentence.contains("indicated for") || sentence.contains("treatment of") || sentence.contains("prevention of")) {
			return true;
		}
		return !sentence.contains("in patients with");
	}

	/** Combined sentence-level exclusions (raw text). */
	private boolean isExcludedSentence(AeSection section, String sentence) {
		if (containsAny(sentence, NEGATION_PATTERNS)) return true;

		List<Pattern> patterns = (section == AeSection.IND) ? IND_EXCLUSION_PATTERNS : AE_EXCLUSION_PATTERNS;
		for (Pattern p : patterns) if (p.matcher(sentence).find()) return true;

		if (section != AeSection.IND) {
			for (Pattern p : STUDY_EXCLUSION_PATTERNS) if (p.matcher(sentence).find()) return true;
			for (Pattern p : BASELINE_HISTORY_PATTERNS) if (p.matcher(sentence).find()) return true;
		}
		return false;
	}

	private static boolean containsAny(String haystack, String[] needles) {
		for (String n : needles) if (haystack.indexOf(n) >= 0) return true;
		return false;
	}

	/**
	 * If two matched keys are antonyms, keep the more specific (by token length).
	 * Uses AM.DB antonym indices (exact + stemmed) plus any manual domain rules.
	 */
	private static void suppressAntonyms(Set<String> foundKeys, String[] tokens) {
		if (foundKeys.isEmpty()) return;

		List<String> keys = new ArrayList<>(foundKeys);
		java.util.function.ToIntFunction<String> tokCount = k -> k.isBlank() ? 0 : k.split("\\s+").length;

		Set<String> toDrop = new HashSet<>();
		for (int i = 0; i < keys.size(); i++) {
			String k1 = keys.get(i);
			if (toDrop.contains(k1)) continue;

			Set<String> antsExact = ANTONYM_INDEX.getOrDefault(k1, Collections.emptySet());
			Set<String> antsStem  = ANTONYM_INDEX_STEMMED.getOrDefault(k1, Collections.emptySet());

			for (int j = i + 1; j < keys.size(); j++) {
				String k2 = keys.get(j);
				if (toDrop.contains(k2)) continue;

				boolean antonymPair = antsExact.contains(k2) || antsStem.contains(k2) ||
						ANTONYM_INDEX.getOrDefault(k2, Collections.emptySet()).contains(k1) ||
						ANTONYM_INDEX_STEMMED.getOrDefault(k2, Collections.emptySet()).contains(k1);

				if (!antonymPair) continue;

				int t1 = tokCount.applyAsInt(k1);
				int t2 = tokCount.applyAsInt(k2);
				if (t1 > t2) toDrop.add(k2);
				else if (t2 > t1) toDrop.add(k1);
			}
		}
		foundKeys.removeAll(toDrop);
	}

	private static void absorbComponents(Set<String> foundKeys) {
		for (Map.Entry<String, String> e : COMPOSITE_ABSORPTION.entrySet()) {
			if (foundKeys.contains(e.getKey())) foundKeys.remove(e.getValue());
		}
	}

	private static final class LabTrigger {
		final Pattern analyte; final Pattern dir; final String key;
		LabTrigger(String analyteRegex, String dirRegex, String dictKey) {
			this.analyte = Pattern.compile(analyteRegex, Pattern.CASE_INSENSITIVE);
			this.dir = Pattern.compile(dirRegex, Pattern.CASE_INSENSITIVE);
			this.key = dictKey;
		}
	}

	private static Set<String> dropGenericWhenSpecific(Set<String> keys) {
		List<List<String>> tokenized = keys.stream().map(k -> List.of(k.split("\\s+"))).toList();
		Set<String> toDrop = new HashSet<>();
		int n = tokenized.size();

		for (int i = 0; i < n; i++) {
			List<String> a = tokenized.get(i);
			String ka = String.join(" ", a);
			for (int j = 0; j < n; j++) {
				if (i == j) continue;
				List<String> b = tokenized.get(j);
				if (b.containsAll(a) && b.size() > a.size()) {
					toDrop.add(ka);
				}
			}
		}
		Set<String> result = new HashSet<>(keys);
		result.removeAll(toDrop);
		return result;
	}

	// ---------------- Per-occurrence local gating ----------------

	/** All occurrence starts for matchedKey in raw sentence; falls back to relaxed regex for stemmed keys. */
	private static List<Integer> findSpanIndices(String sentence, String matchedKey) {
		List<Integer> idxs = new ArrayList<>(2);

		// exact occurrences
		int from = 0, idx;
		while ((idx = sentence.indexOf(matchedKey, from)) >= 0) {
			idxs.add(idx);
			from = idx + matchedKey.length();
		}
		if (!idxs.isEmpty()) return idxs;

		// relaxed: token prefix + word chars (handles stemmed keys)
		String[] toks = matchedKey.split("\\s+");
		if (toks.length == 0) return idxs;

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < toks.length; i++) {
			if (i > 0) sb.append("\\s+");
			sb.append("\\b").append(Pattern.quote(toks[i])).append("\\w*");
		}
		Pattern p = Pattern.compile(sb.toString(), Pattern.CASE_INSENSITIVE);
		java.util.regex.Matcher m = p.matcher(sentence);
		while (m.find()) idxs.add(m.start());
		return idxs;
	}

	private static List<String> orderedTtysFor(AeSection section, List<String> validTty) {
		ArrayList<String> out = new ArrayList<>(validTty.size());
		if (validTty.contains("PT")) out.add("PT");
		if (validTty.contains("LLT")) out.add("LLT");

		if (section == AeSection.IND) {
			for (String t : validTty) if (!out.contains(t)) out.add(t);
		} else {
			for (String t : validTty) {
				if (!out.contains(t) && !"HG".equals(t) && !"HT".equals(t)) out.add(t);
			}
		}
		return out;
	}

	/** Trademark adjacency around a specific occurrence: ®, ™, (R), (TM). */
	private static boolean hasTrademarkAdjacencyAt(String sentence, int start, String matchedKey) {
		int after = start + matchedKey.length();

		// after
		int j = after;
		while (j < sentence.length() && sentence.charAt(j) == ' ') j++;
		if (j < sentence.length()) {
			char ch = sentence.charAt(j);
			if (ch == '\u00AE' || ch == '\u2122') return true; // ® ™
			if (sentence.startsWith("(r)", j) || sentence.startsWith("(tm)", j)) return true;
		}
		// before
		int k = start - 1;
		while (k >= 0 && sentence.charAt(k) == ' ') k--;
		if (k >= 0) {
			char ch = sentence.charAt(k);
			if (ch == '\u00AE' || ch == '\u2122') return true;
			if (k - 2 >= 0) {
				String prev3 = sentence.substring(k - 2, k + 1);
				if ("(r)".equals(prev3) || "(tm)".equals(prev3)) return true;
			}
		}
		return false;
	}

	/** HIV status phrase detector within context (e.g., “… who are HIV negative …”). */
	private static boolean isWithinHivEligibility(String sentence, String matchedKey, int start) {
		int window = 80;
		int s = Math.max(0, start - window);
		int e = Math.min(sentence.length(), start + matchedKey.length() + window);
		String ctx = sentence.substring(s, e);

		boolean whoAreStatus = Pattern.compile("\\bwho\\s+are\\b.*?\\bhiv\\s+(test\\s+)?(negative|positive)\\b",
				Pattern.CASE_INSENSITIVE | Pattern.DOTALL).matcher(ctx).find();

		boolean statusNearby = Pattern.compile("\\bhiv\\s+(test\\s+)?(negative|positive)\\b", Pattern.CASE_INSENSITIVE)
				.matcher(ctx).find();

		return whoAreStatus || statusNearby;
	}

	/** Product/branding detection in a local window around an occurrence. */
	private static boolean isProductBrandingAt(String sentence, int start, String matchedKey) {
		int window = 70;
		int s = Math.max(0, start - window);
		int e = Math.min(sentence.length(), start + matchedKey.length() + window);
		String ctx = sentence.substring(s, e);

		if (PRODUCT_CUE_AFTER.matcher(ctx).find()) return true;

		if (PRODUCT_INSTRUCTION.matcher(ctx).find() && !CLINICAL_CUE.matcher(ctx).find()) return true;

		int firstIdx = sentence.toLowerCase(Locale.ROOT).indexOf(matchedKey.toLowerCase(Locale.ROOT));
		if (firstIdx >= 0 && start > firstIdx + matchedKey.length() + 3) {
			return false;
		}
		return false;
	}

	/** Per-occurrence suppression: trademark, functional “aids”, HIV status, branding, negation/baseline. */
	private static boolean isSuppressedAt(String sentence, int start, String matchedKey) {
		int window = 60;
		int s = Math.max(0, start - window);
		int e = Math.min(sentence.length(), start + matchedKey.length() + window);
		String ctx = sentence.substring(s, e);

		if (hasTrademarkAdjacencyAt(sentence, start, matchedKey)) return true;

		String mkLower = matchedKey.toLowerCase(Locale.ROOT);
		if (("aids".equals(mkLower) || "hiv".equals(mkLower)) &&
				(sentence.contains("aids in the prevention of") || sentence.matches(".*\\b(use|uses)\\s+aids\\b.*"))) {
			return true;
		}

		if (mkLower.contains("hiv") && isWithinHivEligibility(sentence, matchedKey, start)) return true;

		if ("immunodeficiency".equals(mkLower)) {
			int w2 = 40;
			int s2 = Math.max(0, start - w2);
			int e2 = Math.min(sentence.length(), start + matchedKey.length() + w2);
			String c2 = sentence.substring(s2, e2);
			if (Pattern.compile("\\bhuman\\s+immunodeficiency\\s+virus\\b", Pattern.CASE_INSENSITIVE).matcher(c2).find()) {
				return true;
			}
		}

		if (isProductBrandingAt(sentence, start, matchedKey)) return true;

		if (containsAny(ctx, NEGATION_PATTERNS)) return true;

		String mk = Pattern.quote(matchedKey);
		Pattern[] local = new Pattern[] {
				Pattern.compile("\\bhistory of\\s+" + mk, Pattern.CASE_INSENSITIVE),
				Pattern.compile("\\bknown\\s+" + mk, Pattern.CASE_INSENSITIVE),
				Pattern.compile("\\bat baseline\\s+" + mk, Pattern.CASE_INSENSITIVE),
				Pattern.compile(mk + "\\s+at baseline\\b", Pattern.CASE_INSENSITIVE),
				Pattern.compile("\\b(exclusion criteria|were excluded if|patients? (were )?excluded (if|for))\\s+.*?" + mk, Pattern.CASE_INSENSITIVE),
				Pattern.compile(mk + "\\s+.*?\\b(exclusion criteria|were excluded if|patients? (were )?excluded (if|for))\\b", Pattern.CASE_INSENSITIVE)
		};
		for (Pattern p : local) if (p.matcher(ctx).find()) return true;

		return false;
	}

	/** Allow a key if any occurrence within the sentence is not suppressed. */
	private static boolean isAllowedByLocalContext(String rawSentence, String matchedKey) {
		List<Integer> starts = findSpanIndices(rawSentence, matchedKey);
		if (starts.isEmpty()) return true; // cannot locate span – defer to other gates
		for (int st : starts) if (!isSuppressedAt(rawSentence, st, matchedKey)) return true;
		return false;
	}

	private static String normalizePunct(String s) {
		if (s == null) return "";
		String out = s
				.replace('\u2019', '\'').replace('\u2018', '\'').replace('\u201C', '"').replace('\u201D', '"')
				.replace('\u2013', '-').replace('\u2014', '-').replace('\u2212', '-');
		out = out.replaceAll("\\s*'\\s*s\\b", "'s");
		out = out.replaceAll("(?<=\\p{L})-(?=\\p{L})", " ");
		out = out.replaceAll("\\s+(['-])\\s+", "$1");
		return out;
	}

	private static String[] joinPossessives(String[] toks) {
		if (toks == null || toks.length == 0) return toks;
		java.util.ArrayList<String> out = new java.util.ArrayList<>(toks.length);
		int i = 0;
		while (i < toks.length) {
			String t = toks[i];
			if ((t.equals("'s") || t.equals("'S")) && !out.isEmpty()) {
				out.set(out.size() - 1, out.get(out.size() - 1) + "'s");
			} else if (t.equals("'") && i + 1 < toks.length && (toks[i + 1].equalsIgnoreCase("s"))) {
				if (!out.isEmpty()) out.set(out.size() - 1, out.get(out.size() - 1) + "'s");
				i++;
			} else {
				out.add(t);
			}
			i++;
		}
		return out.toArray(new String[0]);
	}


	// ---------------- Stemming ----------------
	private static String[] lemmatizeTokens(String[] tokens) {
		POSTaggerME pos = POS.get();
		DictionaryLemmatizer lemma = sharedLemma();
		if (pos == null || lemma == null || tokens == null) return tokens;
		String[] tags = pos.tag(tokens);
		String[] lemmas = lemma.lemmatize(tokens, tags);
		for (int i = 0; i < lemmas.length; i++) if ("O".equals(lemmas[i])) lemmas[i] = tokens[i];
		return lemmas;
	}

	private static String[] stemTokens(String[] tokens) {
		if (tokens == null || tokens.length == 0) return tokens;
		EnglishStemmer st = STEMMER.get();
		String[] out = new String[tokens.length];
		for (int i = 0; i < tokens.length; i++) {
			String t = tokens[i];
			st.setCurrent(t);
			out[i] = st.stem() ? st.getCurrent() : t;
		}
		return out;
	}

	/** Load AM.DB antonyms once (idempotent). */
	private static void loadAntonymLexicon() {
		InputStream in = null;
		try {
			in = tryOpen(ANTONYM_LEXICON_PATH);
			if (in == null) return; // silently skip; maps remain empty
			try (BufferedReader br = new BufferedReader(new InputStreamReader(in, java.nio.charset.StandardCharsets.UTF_8))) {
				String line;
				EnglishStemmer st = new EnglishStemmer();

				while ((line = br.readLine()) != null) {
					line = line.trim();
					if (line.isEmpty() || line.startsWith("#")) continue;
					if (line.toLowerCase(Locale.ROOT).startsWith("ant-1|eui-1|ant-2|eui-2")) continue;

					String[] cols = line.split("\\|");
					if (cols.length < 10) continue;

					String a1 = normalizeTokenForLex(cols[0]);
					String a2 = normalizeTokenForLex(cols[2]);
					if (a1.isEmpty() || a2.isEmpty() || a1.equals(a2)) continue;

					ANTONYM_INDEX.computeIfAbsent(a1, k -> new HashSet<>()).add(a2);
					ANTONYM_INDEX.computeIfAbsent(a2, k -> new HashSet<>()).add(a1);

					String s1 = stemPhrase(a1, st);
					String s2 = stemPhrase(a2, st);
					ANTONYM_INDEX_STEMMED.computeIfAbsent(s1, k -> new HashSet<>()).add(s2);
					ANTONYM_INDEX_STEMMED.computeIfAbsent(s2, k -> new HashSet<>()).add(s1);
				}
			}
		} catch (Exception ignore) {
			// leave maps empty if load fails
		} finally {
			try { if (in != null) in.close(); } catch (IOException ignore) {}
		}
	}

	/** normalize AM.DB tokens similar to runtime: lowercase, fold curly quotes/dashes, collapse spaces */
	private static String normalizeTokenForLex(String s) {
		if (s == null) return "";
		String out = s.toLowerCase(Locale.ROOT).trim()
				.replace('\u2019', '\'').replace('\u2018', '\'')
				.replace('\u201c', '"').replace('\u201d', '"')
				.replace('\u2013', '-').replace('\u2014', '-').replace('\u2212', '-');
		return out.replaceAll("\\s+", " ");
	}

	/** Stem each token in a space-separated phrase using the class's EnglishStemmer */
	private static String stemPhrase(String phrase, EnglishStemmer st) {
		if (phrase.isEmpty()) return phrase;
		String[] toks = phrase.split("\\s+");
		StringBuilder sb = new StringBuilder(phrase.length());
		for (int i = 0; i < toks.length; i++) {
			String t = toks[i];
			st.setCurrent(t);
			String stem = st.stem() ? st.getCurrent() : t;
			if (i > 0) sb.append(' ');
			sb.append(stem);
		}
		return sb.toString();
	}
}
