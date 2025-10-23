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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.pvlens.spl.umls.UmlsLoader;

class MedDRAProcessorTest {

	// Tiny dictionaries: keys are already lowercase, space-joined tokens
	// TTYs: we’ll pretend we only care about "PT"
	private Map<String, Map<Integer, Map<String, List<String>>>> exactDict;
	private Map<String, Map<Integer, Map<String, List<String>>>> stemmedDict;
	private List<String> validTty;
	private UmlsLoader umls;

	@BeforeEach
	void setUp() {
		validTty = List.of("PT", "LLT"); // include an extra to ensure “missing TTY” path is covered

		// Exact dictionary (no stemming needed)
		// 2-gram: "headache pain" -> AUI A1
		// 1-gram: "rash" -> AUI A2
		Map<Integer, Map<String, List<String>>> exactPT = new HashMap<>();
		exactPT.put(2, Map.of("headache pain", List.of("A1")));
		exactPT.put(1, Map.of("rash", List.of("A2")));
		exactDict = new HashMap<>();
		exactDict.put("PT", exactPT);
		// intentionally do NOT put "LLT" to exercise the “Missing TTY in dictionary:
		// LLT” path

		// Stemmed dictionary:
		// simulate snowball stemming (“headache” -> “headach”, “pains” -> “pain”,
		// “rashes” -> “rash”)
		Map<Integer, Map<String, List<String>>> stemPT = new HashMap<>();
		stemPT.put(2, Map.of("headach pain", List.of("A1")));
		stemPT.put(1, Map.of("rash", List.of("A2")));
		stemmedDict = new HashMap<>();
		stemmedDict.put("PT", stemPT);

		// Mock UmlsLoader to hand these to the processor
		umls = mock(UmlsLoader.class);
		when(umls.getTransformedMap()).thenReturn(exactDict);
		when(umls.getStemmedMap()).thenReturn(stemmedDict);
		when(umls.getValidTty()).thenReturn(validTty);
		when(umls.getMaxTokenMatchLength()).thenReturn(3);
	}

	@Test
	@DisplayName("Exact pass: finds n-grams and unigrams")
	void exactMatch_findsExpected() {
		MedDRAProcessor p = new MedDRAProcessor(umls);

		String text = "Patient reported headache pain and rash.";
		Map<String, List<String>> out = p.processText("AE", text, true);

		assertEquals(Set.of("headache pain", "rash"), out.keySet());
		assertEquals(List.of("A1"), out.get("headache pain"));
		assertEquals(List.of("A2"), out.get("rash"));
	}

	@Test
	@DisplayName("Non-exact pass: stopword filtering + stemming matches stemmed dictionary")
	void nonExactMatch_stemmingWorks() {
		MedDRAProcessor p = new MedDRAProcessor(umls);

		// “headaches pains” should stem roughly to “headach pain”
		String text = "Frequent headaches and pains were noted.";
		Map<String, List<String>> out = p.processText("AE", text, false);
		System.out.println(out);

		assertTrue(out.containsKey("headach pain"), "Expected stemmed bigram to match");
		assertEquals(List.of("A1"), out.get("headach pain"));
	}

	@Test
	@DisplayName("Sentence-level exclusion: 'no evidence of' suppresses matches")
	void exclusion_noEvidence() {
		MedDRAProcessor p = new MedDRAProcessor(umls);

		String text = "There is no evidence of rash or headache pain.";
		Map<String, List<String>> out = p.processText("AE", text, true);

		assertTrue(out.isEmpty(), "Exclusion phrase should block matches");
	}

	@Test
	@DisplayName("Trademark indicative of product name")
	void excludeTrademark() {
		MedDRAProcessor p = new MedDRAProcessor(umls);
		String text = "XYZ ® is a pre-radiation topical cream treatment for the protection against and management of Radiation Dermatitis";
		Map<String, List<String>> out = p.processText("XYZ", text, true);
		assertTrue(out.isEmpty(), "Exclusion phrase should block matches");

	}

	@Test
	@DisplayName("Local negation window around key suppresses match")
	void localNegationBlocks() {
		MedDRAProcessor p = new MedDRAProcessor(umls);

		// Put a negation phrase close to the key (within ~45 chars window)
		String text = "Findings are not indicative of headache pain in this cohort.";
		Map<String, List<String>> out = p.processText("AE", text, true);

		assertFalse(out.containsKey("headache pain"), "Local negation near key should prevent match");
	}

	@Test
	@DisplayName("IND context: only sentences likely to be indications pass initial screen")
	void indicationContextGate() {
		// Minimal mutable dict: TTY "PT" -> 1-gram "rash" -> [AUI]
		Map<String, Map<Integer, Map<String, List<String>>>> dict = new HashMap<>();
		Map<Integer, Map<String, List<String>>> byLen = new HashMap<>();
		Map<String, List<String>> len1 = new HashMap<>();
		len1.put("rash", Collections.singletonList("AUI_RASH"));
		// (optional) include "headache pain" to prove it would match if not filtered by
		// context
		Map<String, List<String>> len2 = new HashMap<>();
		len2.put("headache pain", Collections.singletonList("AUI_HEADACHE_PAIN"));
		byLen.put(1, len1);
		byLen.put(2, len2);
		dict.put("PT", byLen);

		when(umls.getTransformedMap()).thenReturn(dict);
		when(umls.getStemmedMap()).thenReturn(dict); // for non-exact path if used
		when(umls.getValidTty()).thenReturn(List.of("PT"));
		when(umls.getMaxTokenMatchLength()).thenReturn(5);

		MedDRAProcessor p = new MedDRAProcessor(umls);

		// Looks like indication
		String good = "Drug X is indicated for treatment of rash.";
		Map<String, List<String>> ok = p.processText("IND", good, true);
		assertTrue(ok.containsKey("rash"), "Should extract 'rash' in IND context");

		// Likely non-indication sentence — filtered by IND gate before matching
		String bad = "Use in patients with headache pain has not been shown...";
		Map<String, List<String>> blocked = p.processText("IND", bad, true);
		assertTrue(blocked.isEmpty(), "Non-indication phrasing should be filtered early");
	}

	@Test
	@DisplayName("Null/blank input returns empty map")
	void nullAndBlankHandled() {
		MedDRAProcessor p = new MedDRAProcessor(umls);

		assertTrue(p.processText("AE", null, true).isEmpty());
		assertTrue(p.processText("AE", "   ", false).isEmpty());
	}

	@Test
	@DisplayName("Missing TTY in dict does not throw and still matches known TTY")
	void missingTtyIsGraceful() {
		MedDRAProcessor p = new MedDRAProcessor(umls);

		// We declared validTty = ["PT","LLT"] but provided dictionaries only for "PT".
		Map<String, List<String>> out = p.processText("AE", "headache pain", true);

		// Still matches via PT
		assertTrue(out.containsKey("headache pain"));
		// No exception thrown is the main expectation; log message is acceptable.
	}

	@Test
	@DisplayName("Functional 'aids in the prevention of ...' does not trigger disease term")
	void functionalAidsDoesNotMatchDisease() {
		// Build a minimal, fully mutable dictionary:
		// TTY "PT" -> 1-gram -> "aids" -> [AUI]
		Map<String, Map<Integer, Map<String, List<String>>>> dict = new HashMap<>();
		Map<Integer, Map<String, List<String>>> byLen = new HashMap<>();
		Map<String, List<String>> len1 = new HashMap<>();
		len1.put("aids", Collections.singletonList("A_AIDS_PT"));
		byLen.put(1, len1);
		dict.put("PT", byLen);

		// Ensure the MedDRAProcessor sees this dict
		when(umls.getTransformedMap()).thenReturn(dict);

		MedDRAProcessor p = new MedDRAProcessor(umls);

		String text = "Aids in the prevention of dental decay. USE Aids in the prevention of dental decay.";
		Map<String, List<String>> out = p.processText("AE", text, true);

		// "aids" is being used functionally ("assists"), not the disease; should be
		// suppressed
		assertFalse(out.containsKey("aids"), "Functional phrasing should not produce a disease match");
	}

	@Test
	@DisplayName("Brand-like product usage suppresses ARC disease term")
	void brandLikeArcSuppresses() {
		// Dict: PT -> "arc"
		Map<String, Map<Integer, Map<String, List<String>>>> dict = new HashMap<>();
		Map<Integer, Map<String, List<String>>> byLen = new HashMap<>();
		Map<String, List<String>> len1 = new HashMap<>();
		len1.put("arc", Collections.singletonList("AUI_ARC"));
		byLen.put(1, len1);
		dict.put("PT", byLen);

		when(umls.getTransformedMap()).thenReturn(dict);
		// when(umls.getTransformedStemmedMap()).thenReturn(dict);

		MedDRAProcessor p = new MedDRAProcessor(umls);

		String text = "arc is a cream treatment for the protection against and management of radiation dermatitis";
		Map<String, List<String>> out = p.processText("AE", text, true);

		assertTrue(out.isEmpty(), "Brand/product phrasing should suppress ARC as a disease term");
	}

	@Test
	@DisplayName("IND: HIV status and HIV test phrases are excluded; MCD not required")
	void ind_excludes_hiv_status_and_immunodeficiency_in_hiv_phrase() {
		// Minimal dict: only the terms we want to verify suppression on
		Map<String, Map<Integer, Map<String, List<String>>>> dict = new HashMap<>();
		Map<Integer, Map<String, List<String>>> byLen = new HashMap<>();
		Map<String, List<String>> len1 = new HashMap<>();
		len1.put("hiv negative", Collections.singletonList("AUI_HIV_NEG"));
		len1.put("hiv positive", Collections.singletonList("AUI_HIV_POS"));
		len1.put("hiv test negative", Collections.singletonList("AUI_HIV_TNEG"));
		len1.put("hiv test positive", Collections.singletonList("AUI_HIV_TPOS"));
		len1.put("immunodeficiency", Collections.singletonList("AUI_IMMUNODEF"));
		// we deliberately do NOT include MCD because we're only testing filtering here
		byLen.put(2, Map.of("hiv negative", List.of("AUI_HIV_NEG"), "hiv positive", List.of("AUI_HIV_POS")));
		// 1-gram entries
		byLen.put(1, len1);
		dict.put("PT", byLen);

		when(umls.getTransformedMap()).thenReturn(dict);
		when(umls.getStemmedMap()).thenReturn(dict);
		when(umls.getValidTty()).thenReturn(List.of("PT"));
		when(umls.getMaxTokenMatchLength()).thenReturn(5);

		MedDRAProcessor p = new MedDRAProcessor(umls);

		String text = "Product XYZ is indicated for the treatment of patients with multicentric Castleman's disease "
				+ "who are human immunodeficiency virus (HIV) negative and human herpesvirus-8 (HHV-8) negative. "
				+ "Limitations of Use XYZ was not studied in patients with MCD who are HIV positive or HHV-8 positive "
				+ "because XYZ did not bind to virally produced IL-6 in a nonclinical study.";

		Map<String, List<String>> out = p.processText("IND", text, true);

		// Nothing HIV-status related should appear
		assertFalse(out.keySet().stream().anyMatch(k -> k.contains("hiv")),
				"HIV status terms must be excluded in IND context");
		assertFalse(out.containsKey("immunodeficiency"),
				"Immunodeficiency inside 'human immunodeficiency virus' must be suppressed");
	}

	@Test
	@DisplayName("IND: matches AIDS-related Kaposi’s sarcoma (PT 10023286) with Unicode punctuation")
	void ind_matchesAidsRelatedKaposisSarcoma() {
		// Build a minimal dictionary that reflects loader output AFTER rotation:
		// PT → 4-gram "aids related kaposi's sarcoma" → AUI for 10023286
		Map<String, Map<Integer, Map<String, List<String>>>> dict = new HashMap<>();
		Map<Integer, Map<String, List<String>>> byLen = new HashMap<>();
		Map<String, List<String>> len4 = new HashMap<>();
		len4.put("aids related kaposi's sarcoma", Collections.singletonList("AUI_10023286"));
		byLen.put(4, len4);
		dict.put("PT", byLen);

		// Point the mocked UmlsLoader at this dictionary
		when(umls.getTransformedMap()).thenReturn(dict);
		when(umls.getStemmedMap()).thenReturn(dict); // exact pass is sufficient, but keep symmetry
		when(umls.getValidTty()).thenReturn(List.of("PT"));
		when(umls.getMaxTokenMatchLength()).thenReturn(6);

		MedDRAProcessor p = new MedDRAProcessor(umls);

		// Use curly apostrophe and an en dash to exercise normalization:
		// "Kaposi’s" (U+2019) and "AIDS–related" (U+2013)
		String indicationText = "XYZ is indicated for the second-line treatment of AIDS–related Kaposi’s sarcoma.";

		Map<String, List<String>> out = p.processText("IND", indicationText, true);

		assertTrue(out.containsKey("aids related kaposi's sarcoma"),
				"Should match AIDS-related Kaposi's sarcoma in IND context");
		assertEquals(List.of("AUI_10023286"), out.get("aids related kaposi's sarcoma"),
				"Should map to PT 10023286 AUI");
	}

//
// TO-DO:
//    
// This test currently fails. It is complicated to get this test and indicationContextGate() to both pass
// at the same time and should be the subject of future research and development
//
//    @Test
//    @DisplayName("First occurrence branded, second clinical usage allowed")
//    void brandedThenClinicalAllowed() {
//        Map<String, Map<Integer, Map<String, List<String>>>> dict = new HashMap<>();
//        Map<Integer, Map<String, List<String>>> byLen = new HashMap<>();
//        Map<String, List<String>> len1 = new HashMap<>();
//        len1.put("arc", Collections.singletonList("AUI_ARC"));
//        byLen.put(1, len1);
//        dict.put("PT", byLen);
//
//        when(umls.getTransformedMap()).thenReturn(dict);
//        // (exact pass is enough for this test)
//        MedDRAProcessor p = new MedDRAProcessor(umls);
//
//        String text = "ARC is a topical cream. ARC is used to treat ARC.";
//        Map<String, List<String>> out = p.processText("AE", text, true);
//
//        // The first ARC (branding) suppressed; later clinical usage should be identified
//        assertTrue(out.containsKey("arc"), "Later clinical usage of ARC should still be identified");
//    }

	// --- Helper to build the nested transformed map structure ---
	private Map<String, Map<Integer, Map<String, List<String>>>> dictOf(Object... entries) {
		// entries: tty, ngramKey, aui, tty, ngramKey, aui, ...
		Map<String, Map<Integer, Map<String, List<String>>>> top = new HashMap<>();
		for (int i = 0; i < entries.length; i += 3) {
			String tty = (String) entries[i];
			String key = (String) entries[i + 1];
			String aui = (String) entries[i + 2];
			int tokLen = key.trim().isEmpty() ? 0 : key.split("\\s+").length;

			top.computeIfAbsent(tty, __ -> new HashMap<>()).computeIfAbsent(tokLen, __ -> new HashMap<>())
					.computeIfAbsent(key, __ -> new ArrayList<>()).add(aui);
		}
		return top;
	}

	// --- Test 1: Antonym guard (non-small cell vs small cell) ---
	@Test
	@DisplayName("IND: Non-small cell keeps; Small cell suppressed by antonym guard")
	void ind_antonymGuard_nsclcKeeps_sclcDropped() {
		// Mock dictionary contains both concepts
		var umls = mock(UmlsLoader.class);
		Map<String, Map<Integer, Map<String, List<String>>>> transformed = dictOf("PT", "non small cell lung cancer",
				List.of("AUI_NSCLC").toString(), "PT", "small cell lung cancer", List.of("AUI_SCLC").toString());
		when(umls.getTransformedMap()).thenReturn(transformed);
		when(umls.getStemmedMap()).thenReturn(transformed); // not used for exact=true
		when(umls.getValidTty()).thenReturn(List.of("PT"));
		when(umls.getMaxTokenMatchLength()).thenReturn(6);

		MedDRAProcessor p = new MedDRAProcessor(umls);

		// Contains an explicit "non–small cell lung cancer" (with en dash)
		String text = "TAXOL, in combination with cisplatin, is indicated for the first-line treatment of non–small cell lung cancer.";
		Map<String, List<String>> out = p.processText("IND", text, true);

		assertTrue(out.containsKey("non small cell lung cancer"), "Should match 'non small cell lung cancer'");
		assertFalse(out.containsKey("small cell lung cancer"),
				"Antonym guard should suppress 'small cell lung cancer' when NSCLC is present");
	}

	// --- Test 2: Composite absorption (AIDS-related Kaposi's sarcoma) ---
	@Test
	@DisplayName("IND: Composite absorbs its parts (AIDS-related Kaposi’s sarcoma)")
	void ind_compositeAbsorbsParts() {
		var umls = mock(UmlsLoader.class);
		Map<String, Map<Integer, Map<String, List<String>>>> transformed = dictOf("PT", "aids related kaposi's sarcoma",
				List.of("AUI_KS_AIDS").toString(), "PT", "kaposi's sarcoma", List.of("AUI_KS").toString(), "PT",
				"sarcoma", List.of("AUI_SARC").toString(), "PT", "aids", List.of("AUI_AIDS").toString(), "LLT",
				"acquired immunodeficiency syndrome", List.of("AUI_AIDS_LLT").toString());
		when(umls.getTransformedMap()).thenReturn(transformed);
		when(umls.getStemmedMap()).thenReturn(transformed);
		when(umls.getValidTty()).thenReturn(List.of("PT", "LLT"));
		when(umls.getMaxTokenMatchLength()).thenReturn(6);

		MedDRAProcessor p = new MedDRAProcessor(umls);

		// Unicode punctuation variants: "AIDS–related Kaposi’s sarcoma"
		String text = "TAXOL is indicated for the second-line treatment of AIDS–related Kaposi’s sarcoma.";
		Map<String, List<String>> out = p.processText("IND", text, true);

		assertTrue(out.containsKey("aids related kaposi's sarcoma"), "Composite should be kept");
		assertFalse(out.containsKey("kaposi's sarcoma"),
				"Standalone 'kaposi's sarcoma' should be absorbed by the composite");
		assertFalse(out.containsKey("sarcoma"), "Generic 'sarcoma' should be absorbed by the composite");
		assertFalse(out.containsKey("aids"), "Modifier 'AIDS' should be absorbed by the composite");
		assertFalse(out.containsKey("acquired immunodeficiency syndrome"),
				"LLT for AIDS should also be absorbed when composite is present");
	}

	// --- Test 3: Specificity drop (keep 'breast cancer', drop 'cancer') ---
	@Test
	@DisplayName("IND: Specificity ranking drops generic 'cancer' when site-specific exists")
	void ind_dropGenericWhenSpecificExists() {
		var umls = mock(UmlsLoader.class);
		Map<String, Map<Integer, Map<String, List<String>>>> transformed = dictOf("PT", "breast cancer",
				List.of("AUI_BREAST").toString(), "LLT", "cancer", List.of("AUI_CANCER").toString());
		when(umls.getTransformedMap()).thenReturn(transformed);
		when(umls.getStemmedMap()).thenReturn(transformed);
		when(umls.getValidTty()).thenReturn(List.of("PT", "LLT"));
		when(umls.getMaxTokenMatchLength()).thenReturn(4);

		MedDRAProcessor p = new MedDRAProcessor(umls);

		String text = "TAXOL is indicated for the treatment of breast cancer after failure of combination chemotherapy for metastatic disease.";
		Map<String, List<String>> out = p.processText("IND", text, true);

		assertTrue(out.containsKey("breast cancer"), "Specific term should be kept");
		assertFalse(out.containsKey("cancer"),
				"Generic 'cancer' should be dropped when a specific site term is present");
	}

	/*
	 * ----------------------------------------------------------- 
	 * AE polarity: suppress “normal” findings
	 * -----------------------------------------------------------
	 */
	@Test
	@DisplayName("AE: suppresses 'electrocardiogram normal' (polarity normal) in AE context")
	void ae_polarity_suppresses_normal_findings() {
		var umls = mock(UmlsLoader.class);

		// Only "electrocardiogram normal" exists in dictionary for this test.
		Map<String, Map<Integer, Map<String, List<String>>>> transformed = dictOf("PT", "electrocardiogram normal",
				"AUI_ECG_NORM");

		when(umls.getTransformedMap()).thenReturn(transformed);
		when(umls.getStemmedMap()).thenReturn(transformed);
		when(umls.getValidTty()).thenReturn(List.of("PT"));
		when(umls.getMaxTokenMatchLength()).thenReturn(4);

		MedDRAProcessor p = new MedDRAProcessor(umls);

		String text = "During treatment the electrocardiogram was normal.";
		Map<String, List<String>> out = p.processText("AE", text, true);

		assertFalse(out.containsKey("electrocardiogram normal"),
				"AE polarity rule should suppress 'electrocardiogram normal'");
	}

	/*
	 * AE skips HT/HG groupers
	 */
	@Test
	@DisplayName("AE: skips HG/HT groupers even if they match")
	void ae_skips_hg_ht_groupers() {
		var umls = mock(UmlsLoader.class);

		// Provide an HG/HT key that matches the text; no PT/LLT equivalent on purpose.
		Map<String, Map<Integer, Map<String, List<String>>>> transformed = dictOf("HT",
				"upper respiratory tract infections", "AUI_URTI_HT");

		when(umls.getTransformedMap()).thenReturn(transformed);
		when(umls.getStemmedMap()).thenReturn(transformed);
		// Include HG/HT in valid tty list to prove we actively skip them in AE.
		when(umls.getValidTty()).thenReturn(List.of("PT", "LLT", "HG", "HT"));
		when(umls.getMaxTokenMatchLength()).thenReturn(5);

		MedDRAProcessor p = new MedDRAProcessor(umls);

		String text = "Adverse events included upper respiratory tract infections.";
		Map<String, List<String>> out = p.processText("AE", text, true);

		// It matched as HT, but AE output should skip HG/HT → nothing emitted.
		assertTrue(out.isEmpty(), "AE should skip HG/HT groupers in the output");
	}

	/*
	 * ----------------------------------------------------------- 
	 * baseline/population diseases (in patients with)
	 * -----------------------------------------------------------
	 */
	@Test
	@DisplayName("AE: filters indication/population diseases in AE ('in patients with ...')")
	void ae_filters_population_diseases_in_patients_with() {
		var umls = mock(UmlsLoader.class);

		Map<String, Map<Integer, Map<String, List<String>>>> transformed = dictOf("PT", "ovarian cancer", "AUI_OV_CA");

		when(umls.getTransformedMap()).thenReturn(transformed);
		when(umls.getStemmedMap()).thenReturn(transformed);
		when(umls.getValidTty()).thenReturn(List.of("PT"));
		when(umls.getMaxTokenMatchLength()).thenReturn(3);

		MedDRAProcessor p = new MedDRAProcessor(umls);

		String text = "In patients with ovarian cancer receiving paclitaxel, adverse events were recorded.";
		Map<String, List<String>> out = p.processText("AE", text, true);

		assertFalse(out.containsKey("ovarian cancer"),
				"Population/indication disease should be filtered from AE when preceded by 'in patients with'");
	}

	/*
	 * -----------------------------------------------------------
	 * lab elevation → PT (only if present in dict)
	 * -----------------------------------------------------------
	 */
	@Test
	@DisplayName("AE: maps 'elevated bilirubin' to 'blood bilirubin increased' PT when available")
	void ae_lab_elevation_maps_to_bilirubin_increased() {
		var umls = mock(UmlsLoader.class);

		// Provide the exact PT key the helper looks for
		Map<String, Map<Integer, Map<String, List<String>>>> transformed = dictOf("PT", "blood bilirubin increased",
				"AUI_BILI_INC");

		when(umls.getTransformedMap()).thenReturn(transformed);
		when(umls.getStemmedMap()).thenReturn(transformed);
		when(umls.getValidTty()).thenReturn(List.of("PT"));
		when(umls.getMaxTokenMatchLength()).thenReturn(3);

		MedDRAProcessor p = new MedDRAProcessor(umls);

		String text = "Laboratory findings showed elevated bilirubin during treatment.";
		Map<String, List<String>> out = p.processText("AE", text, true);

		assertTrue(out.containsKey("blood bilirubin increased"),
				"Should surface 'blood bilirubin increased' when text says 'elevated bilirubin' and PT exists");
	}

}
