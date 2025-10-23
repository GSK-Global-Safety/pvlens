package org.pvlens.spl.om;

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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import org.junit.jupiter.api.Test;
import org.pvlens.spl.umls.Atom;
import org.pvlens.spl.umls.UmlsLoader;

class SplDrugTest {

	// ---- helpers -------------------------------------------------------------
	private static final SimpleDateFormat DF = new SimpleDateFormat("yyyy-MM-dd");
	
	private static Atom atom(String aui, String term, String cui, String tty, String code, String ptCode) {
		Atom a = new Atom();
		a.setAui(aui);
		a.setTerm(term);
		a.setCui(cui);
		a.setTty(tty);
		a.setCode(code);
		a.setPtCode(ptCode);
		a.setDatabaseId(Math.abs(aui.hashCode()));
		return a;
	}

	private static Date d(int y, int m, int d) {
		Calendar c = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		c.clear();
		c.set(y, m - 1, d, 0, 0, 0);
		return c.getTime();
	}
	
	private static Date d(String ymd) {
		try {
			return DF.parse(ymd);
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
	}


	private Atom mockAtom(String aui, String cui, String code, String tty, String ptCode) {
		Atom a = mock(Atom.class);
		when(a.getAui()).thenReturn(aui);
		when(a.getCui()).thenReturn(cui);
		when(a.getCode()).thenReturn(code);
		when(a.getTty()).thenReturn(tty);
		when(a.getPtCode()).thenReturn(ptCode);
		when(a.getTerm()).thenReturn("T-" + aui);
		return a;
	}

	@Test
	void normalizeNdc_formats() {
		assertEquals("012345678", SplDrug.normalizeNdc("1234-5678")); // 4-4 -> 0 + 4 + 4
		assertEquals("123456789", SplDrug.normalizeNdc("12345-6789")); // 5-4 -> concat
		assertEquals("", SplDrug.normalizeNdc("12345-67-89")); // unsupported formats -> ""
		assertEquals("", SplDrug.normalizeNdc("garbage"));
	}

	@Test
	void xmlFiles_areDefensive_andAppendable() {
		SplDrug d = new SplDrug();

		HashMap<String, Boolean> original = new HashMap<String, Boolean>();
		original.put("a.xml", false);
		d.setXmlFiles(original);

		// mutate source list; internal list must not change
		original.put("b.xml", false);
		assertEquals(List.of("a.xml"), d.getXmlFilesAsList());

		d.addXmlFile("c.xml", false);
		assertEquals(List.of("c.xml", "a.xml"), d.getXmlFilesAsList());
	}

	@Test
	void multiCuiKey_isSorted_andDeduped() {
		SplDrug d = new SplDrug();
		d.addDrugProductCui("C2");
		d.addDrugProductCui("C1");
		d.addDrugProductCui("C1"); // dup ignored
		assertEquals("C1|C2|", d.getMultiCuiKey());
	}

	@Test
	void snomedParents_addOnce_andReset() {
		SplDrug d = new SplDrug();
		d.addSnomedParent("A1");
		d.addSnomedParent("A1");
		d.addSnomedParent("B2");
		assertEquals(2, d.getSnomedParentAuis().size());
		d.resetParents();
		assertTrue(d.getSnomedParentAuis().isEmpty());
	}

	@Test
	void hasExactRxNormIngredients_biDirectional() {
		SplDrug d1 = new SplDrug();
		SplDrug d2 = new SplDrug();

		// IN/PIN drive the ingredient comparison
		Atom in1 = mockAtom("A-IN-1", "CUI1", "X", "IN", "PTX");
		Atom in2 = mockAtom("A-IN-2", "CUI2", "Y", "PIN", "PTY");

		d1.addRxNormPt(in1);
		d1.addRxNormPt(in2);

		d2.addRxNormPt(in1);
		d2.addRxNormPt(in2);

		assertTrue(d1.hasExactRxNormIngredients(d2));

		// remove one from d2 -> now not exact
		d2.getRxNormPts().remove(in2.getAui());
		assertFalse(d1.hasExactRxNormIngredients(d2));
	}

	@Test
	void hasSnomedParents_allContained() {
		SplDrug d1 = new SplDrug();
		SplDrug d2 = new SplDrug();

		// d1 has P1,P2; d2 has P1 -> d1 contains all parents of d2 => true
		d1.addSnomedParent("P1");
		d1.addSnomedParent("P2");
		d2.addSnomedParent("P1");
		assertTrue(d1.hasSnomedParents(d2));

		// reverse: d2 does not contain all parents of d1 => false
		assertFalse(d2.hasSnomedParents(d1));

		// add missing parent to d2 -> now d2 contains all parents of d1 => true
		d2.addSnomedParent("P2");
		assertTrue(d2.hasSnomedParents(d1));
	}

	@Test
	void guidXmlMaps_includesMerged() {
		SplDrug d = new SplDrug();
		d.setGuid("G1");
		d.addXmlFile("g1.xml", false);

		Map<String, List<String>> merged = new HashMap<>();
		merged.put("G2", List.of("g2a.xml", "g2b.xml"));
		d.setMergedGuidXmlPairs(new HashMap<>(merged));

		Map<String, List<String>> all = d.getGuidXmlMaps();
		assertEquals(Set.of("G1", "G2"), all.keySet());
		assertEquals(List.of("g1.xml"), all.get("G1"));
		assertEquals(List.of("g2a.xml", "g2b.xml"), all.get("G2"));
	}

	@Test
	void ndcHelpers_detectAndNormalize() {
		SplDrug d = new SplDrug();
		Atom dp = mockAtom("DP1", "C", "12345-6789", "MTHSPL", "PT");
		d.getDrugProduct().put(dp.getAui(), dp);

		assertTrue(d.hasNdcCodes(List.of("999-9999", "12345-6789")));
		assertEquals(List.of("12345-6789"), d.getNdcCodes());
		assertEquals(List.of("123456789"), d.getNormalizedNdcCodes());
	}

	@Test
	void nda_set_and_getters() {
		SplDrug d = new SplDrug();
		d.setGuid("G1");
		d.setNda(13579);
		assertEquals(13579, d.getPrimaryNda());
		assertEquals(List.of(13579), d.getNdaIds());
	}

	@Test
	void copySplDrug_isDeepEnough_forCollections() {
		SplDrug d = new SplDrug();
		d.setGuid("G1");
		d.addXmlFile("a.xml", false);
		d.addDrugProductCui("C1");

		Atom a = mockAtom("A1", "CUI", "CODE", "IN", "PT");
		d.getDrugProduct().put(a.getAui(), a);

		SplDrug copy = d.copySplDrug();

		// mutate original collections
		d.addXmlFile("b.xml", false);
		d.getDrugProductCuis().add("C2");
		d.getDrugProduct().clear();

		// copy stays unchanged
		assertEquals(List.of("a.xml"), copy.getXmlFilesAsList());
		assertEquals(List.of("C1"), copy.getDrugProductCuis());
		assertEquals(1, copy.getDrugProduct().size());
	}

	@Test
	void getAeCuis_and_getIndCuis_fromOutcomeSources() {
		// prepare drug
		SplDrug d = new SplDrug();
		d.setGuid("G1");

		// put AEs for G1 into outcome sources directly
		d.getExactMatchWarnings().getOutcomeSource().put("G1", List.of("AUI_W1", "AUI_W2"));
		d.getExactMatchIndications().getOutcomeSource().put("G1", List.of("AUI_I1"));

		// fake umls with AUI->Atom->CUI map
		UmlsLoader umls = mock(UmlsLoader.class);
		Map<String, Atom> meddra = new HashMap<>();
		meddra.put("AUI_W1", mockAtom("AUI_W1", "CUI_W1", "x", "PT", "p"));
		meddra.put("AUI_W2", mockAtom("AUI_W2", "CUI_W2", "x", "PT", "p"));
		meddra.put("AUI_I1", mockAtom("AUI_I1", "CUI_I1", "x", "PT", "p"));
		when(umls.getMedDRA()).thenReturn(meddra);

		List<String> aeCuis = d.getAeCuis(umls, "G1");
		List<String> indCuis = d.getIndCuis(umls, "G1");

		assertEquals(Set.of("CUI_W1", "CUI_W2"), new HashSet<>(aeCuis));
		assertEquals(List.of("CUI_I1"), indCuis);
	}

	@Test
	void labelWindows_detection() {
		SplDrug d = new SplDrug();
		d.setGuid("G1");

		// one AE warning on 2020-06-01; one blackbox on 2021-01-01
		Calendar c = Calendar.getInstance();
		c.set(2020, Calendar.JUNE, 1, 0, 0, 0);
		Date d20200601 = c.getTime();
		c.set(2021, Calendar.JANUARY, 1, 0, 0, 0);
		Date d20210101 = c.getTime();
		c.set(2019, Calendar.JANUARY, 1, 0, 0, 0);
		Date winStart = c.getTime();
		c.set(2020, Calendar.DECEMBER, 31, 23, 59, 59);
		Date winEnd = c.getTime();

		Atom w = mockAtom("W1", "CW", "x", "PT", "p");
		Atom b = mockAtom("B1", "CB", "x", "PT", "p");

		d.getExactMatchWarnings().getCodes().add(w);
		d.getExactMatchWarnings().getFirstAdded().put("W1", d20200601);

		d.getExactMatchBlackbox().getCodes().add(b);
		d.getExactMatchBlackbox().getFirstAdded().put("B1", d20210101);

		assertTrue(d.hasNewLabel(winStart, winEnd)); // caught warning in window

		List<Atom> warn = d.getWarningLabelEvent(winStart, winEnd);
		assertEquals(List.of(w), warn);

		List<Atom> box = d.getBlackboxLabelEvent(winStart, winEnd);
		assertTrue(box.isEmpty());
	}

	@Test
	void hasDataPrior_checksAnyOutcomeBefore() {
		SplDrug d = new SplDrug();
		d.setGuid("G1");

		Calendar c = Calendar.getInstance();
		c.set(2018, Calendar.JANUARY, 1);
		Date cutoff = c.getTime();
		c.set(2017, Calendar.JUNE, 1);
		Date earlier = c.getTime();

		Atom ind = mockAtom("I1", "CI", "x", "PT", "p");
		d.getExactMatchIndications().getCodes().add(ind);
		d.getExactMatchIndications().getFirstAdded().put("I1", earlier);

		assertTrue(d.hasDataPrior(cutoff));
	}

	@Test
	void llt_and_pt_share_minimum_via_pt_code_even_with_different_cui() {
	    SplDrug spl = new SplDrug();
	    Outcome out = new Outcome(); // or spl.getExactMatchWarnings() etc.

	    // PT atom (true PT): same PT code for code and ptCode; CUI = C_PT
	    Atom pt = new Atom();
	    pt.setAui("PT_AUI");
	    pt.setCui("C_PT");
	    pt.setCode("PT123");
	    pt.setPtCode("PT123");

	    // LLT atom: different CUI but mapped to the PT via ptCode
	    Atom llt = new Atom();
	    llt.setAui("LLT_AUI");
	    llt.setCui("C_LLT");          // different CUI
	    llt.setCode("LLT999");        // LLT code
	    llt.setPtCode("PT123");       // PT mapping

	    // Later date on PT, earlier date on LLT
	    Date later = d("2020-04-01");
	    Date earlier = d("2018-01-15");

	    out.getCodes().add(pt);
	    out.getFirstAdded().put("PT_AUI", later);

	    out.getCodes().add(llt);
	    out.getFirstAdded().put("LLT_AUI", earlier);

	    // Reconcile in-bucket
	    SplDrug.reconcileOutcomeMinDates(out);

	    // Expect both AUIs to get the earlier date via shared PT code
	    assertEquals(earlier, out.getFirstAdded().get("PT_AUI"));
	    assertEquals(earlier, out.getFirstAdded().get("LLT_AUI"));
	}

	
	
	@Test
	void mergeProductGroup_matchesOnParents_orCuis_orNda() {
		SplDrug a = new SplDrug();
		a.setGuid("GUI01");
		
		SplDrug b = new SplDrug();
		b.setGuid("GUI02");

		// Case 1: SNOMED parent single match
		a.resetParents();
		b.resetParents();
		a.addSnomedParent("SP1");
		b.addSnomedParent("SP1");
		assertTrue(a.mergeProductGroup(b));

		// Case 2: exact drug product CUI match (bidirectional)
		SplDrug c = new SplDrug();
		c.setGuid("GUI03");

		SplDrug d = new SplDrug();
		d.setGuid("GUI04");

		c.addDrugProductCui("CUI1");
		d.addDrugProductCui("CUI1");
		assertTrue(c.mergeProductGroup(d));

		// Case 3: NDA overlap
		SplDrug e = new SplDrug();
		SplDrug f = new SplDrug();
		e.setGuid("GE");
		f.setGuid("GF");
		e.setNda(12345);
		f.setNda(12345);
		assertTrue(e.mergeProductGroup(f));
	}

	@Test
	void getPrimaryCuis_collectsFromDPandRxNorm() {
		SplDrug d = new SplDrug();
		d.addDrugProductCui("CUI_DP");
		d.addRxNormPt(mockAtom("RX1", "CUI_RX_SBD", "x", "SBD", "p")); // contributes
		d.addRxNormPt(mockAtom("RX2", "CUI_RX_IN", "x", "IN", "p")); // not SBD/PSN -> ignored
		d.addRxNormPt(mockAtom("RX3", "CUI_RX_PSN", "x", "PSN", "p")); // contributes

		List<String> cuis = d.getPrimaryCuis();
		assertTrue(cuis.contains("CUI_DP"));
		assertTrue(cuis.contains("CUI_RX_SBD"));
		assertTrue(cuis.contains("CUI_RX_PSN"));
		assertFalse(cuis.contains("CUI_RX_IN"));
	}

	@Test
	void addSnomedPt_and_addIngedient_store_by_aui() {
		SplDrug drug = new SplDrug();

		Atom snomedPt = atom("A-SN1", "Some SNOMED PT", "C-SN1", "PT", "S-CODE", "S-PT");
		Atom ingredient = atom("A-ING1", "An Ingredient", "C-ING1", "IN", "I-CODE", "I-PT");

		drug.addSnomedPt(snomedPt);
		drug.addIngedient(ingredient);

		assertTrue(drug.getSnomedPts().containsKey("A-SN1"));
		assertSame(snomedPt, drug.getSnomedPts().get("A-SN1"));

		assertTrue(drug.getIngredients().containsKey("A-ING1"));
		assertSame(ingredient, drug.getIngredients().get("A-ING1"));
	}

	@Test
	void updateOutcome_nonSrlc_via_updateLabels_merges_sources_and_dates_and_harmonizes_by_cui() {
		// Target drug we will update
		SplDrug target = new SplDrug();

		// Source drug carrying outcomes (non-SRLC path)
		SplDrug source = new SplDrug();

		Outcome srcWarn = new Outcome();
		srcWarn.setWarning(true);

		// Two atoms that share the same CUI; different dates
		Atom a1 = atom("A1", "Headache", "C-HEAD", "PT", "CODE-1", "PT-1");
		Atom a2 = atom("A2", "Head pain", "C-HEAD", "PT", "CODE-2", "PT-2");

		Date later = d(2022, 6, 20);
		Date earlier = d(2021, 12, 31);

		// Put both into the source outcome with distinct sources and dates
		srcWarn.addCode("ZIP-ONE", a1, later);
		srcWarn.addCode("ZIP-TWO", a2, earlier);

		source.setExactMatchWarnings(srcWarn);

		// Act: merge outcomes (srlcUpdate = false path inside updateOutcome)
		target.updateLabels(source);

		Outcome merged = target.getExactMatchWarnings();

		// Codes are present
		assertTrue(merged.getCodes().contains(a1));
		assertTrue(merged.getCodes().contains(a2));

		// Sources merged and preserved per AUI
		assertEquals(List.of("ZIP-ONE"), merged.getSources(a1.getAui()));
		assertEquals(List.of("ZIP-TWO"), merged.getSources(a2.getAui()));

		// updateLabelDates() should propagate the earliest date among same-CUI atoms to
		// both
		Date dA1 = merged.getFirstAdded().get(a1.getAui());
		Date dA2 = merged.getFirstAdded().get(a2.getAui());
		assertEquals(earlier, dA1);
		assertEquals(earlier, dA2);
	}

	@Test
	void updateOutcome_srlc_true_via_updateLabels_Srlc_preserves_current_sources_and_adds_new_aui_with_guid_source() {
		// Prepare a drug with an existing outcome
		SplDrug drug = new SplDrug();
		drug.setGuid("GUID-123"); // needed for the "new atom" path to set source = this GUID

		Outcome current = new Outcome();
		current.setWarning(true);

		Atom existing = atom("AX", "Dizziness", "C-DIZ", "PT", "CODE-X", "PT-X");
		Date currentDate = d(2023, 1, 15);
		current.addCode("ZIP-OLD", existing, currentDate);

		drug.setExactMatchWarnings(current);

		// SRLC outcome: same AUI with earlier date (should update date but keep current
		// source),
		// plus a brand-new AUI which should be added with source = GUID-123
		Outcome srlcWarn = new Outcome();
		srlcWarn.setWarning(true);

		Date earlier = d(2022, 5, 10);
		srlcWarn.addCode("SRLC", existing, earlier); // same AUI, earlier date

		Atom newAtom = atom("AN", "Nausea", "C-NAU", "PT", "CODE-N", "PT-N");
		Date newDate = d(2024, 2, 1);
		srlcWarn.addCode("SRLC", newAtom, newDate);

		// Build minimal SRLC model (assuming Lombok @Data on Srlc)
		Srlc srlc = new Srlc();
		srlc.setExactAeMatch(srlcWarn);
		srlc.setExactBlackboxMatch(new Outcome());
		srlc.setNlpAeMatch(new Outcome());
		srlc.setNlpBlackboxMatch(new Outcome());
		srlc.setDrugName("42");
		srlc.setApplicationNumber(999);

		// Act: SRLC update (srlcUpdate = true path)
		drug.updateLabels(srlc);

		Outcome updated = drug.getExactMatchWarnings();

		// 1) Existing atom date should be updated to the earlier SRLC date,
		// and original source ZIP-OLD must still be present (preserve SPL source).
		assertEquals(earlier, updated.getFirstAdded().get(existing.getAui()));
		assertEquals(List.of("ZIP-OLD"), updated.getSources(existing.getAui()));

		// 2) New atom should be added, with source == drug GUID
		assertTrue(updated.getCodes().contains(newAtom));
		assertEquals(List.of("GUID-123"), updated.getSources(newAtom.getAui()));
		assertEquals(newDate, updated.getFirstAdded().get(newAtom.getAui()));
	}

	// ------- getAtcClasses / hasExactAtcClass --------------------------------

	@Test
	void getAtcClasses_returns_values_of_atcCodes_map() {
		SplDrug d = new SplDrug();

		// Simulate two NDCs each mapped to an ATC Atom
		Atom atc1 = atom("ATC-A", "ATC One", "C-ATC1", "ATC", "A01", "P1");
		Atom atc2 = atom("ATC-B", "ATC Two", "C-ATC2", "ATC", "B02", "P2");

		d.getAtcCodes().put("12345-6789", atc1);
		d.getAtcCodes().put("55555-4444", atc2);

		List<Atom> classes = d.getAtcClasses();

		// Order not guaranteed (HashMap); assert by set
		assertEquals(Set.of(atc1, atc2), new HashSet<>(classes));
	}

	@Test
	void hasExactAtcClass_false_when_any_side_empty() {
		SplDrug a = new SplDrug();
		SplDrug b = new SplDrug();

		Atom atc = atom("ATC-X", "ATC X", "C-X", "ATC", "X01", "PX");

		a.getAtcCodes().put("11111-1111", atc);
		// b has none

		assertFalse(a.hasExactAtcClass(b));
		assertFalse(b.hasExactAtcClass(a));

		// both empty
		SplDrug c = new SplDrug();
		SplDrug d = new SplDrug();
		assertFalse(c.hasExactAtcClass(d));
	}

	@Test
	void hasExactAtcClass_true_when_both_have_identical_set() {
		SplDrug a = new SplDrug();
		SplDrug b = new SplDrug();

		// Use the SAME Atom instances so equality is guaranteed regardless of
		// Atom.equals
		Atom atc1 = atom("ATC-1", "ATC 1", "C1", "ATC", "A01", "P1");
		Atom atc2 = atom("ATC-2", "ATC 2", "C2", "ATC", "B02", "P2");

		a.getAtcCodes().put("11111-1111", atc1);
		a.getAtcCodes().put("22222-2222", atc2);

		b.getAtcCodes().put("99999-9999", atc1);
		b.getAtcCodes().put("88888-8888", atc2);

		assertTrue(a.hasExactAtcClass(b));
		assertTrue(b.hasExactAtcClass(a));
	}

	@Test
	void hasExactAtcClass_false_when_sets_differ() {
		SplDrug a = new SplDrug();
		SplDrug b = new SplDrug();

		Atom atc1 = atom("ATC-1", "ATC 1", "C1", "ATC", "A01", "P1");
		Atom atc2 = atom("ATC-2", "ATC 2", "C2", "ATC", "B02", "P2");
		Atom atc3 = atom("ATC-3", "ATC 3", "C3", "ATC", "C03", "P3");

		a.getAtcCodes().put("11111-1111", atc1);
		a.getAtcCodes().put("22222-2222", atc2);

		b.getAtcCodes().put("33333-3333", atc1);
		b.getAtcCodes().put("44444-4444", atc3);

		assertFalse(a.hasExactAtcClass(b));
		assertFalse(b.hasExactAtcClass(a));
	}

	// ------- getAllIndCuis ---------------------------------------------------

	@Test
	void getAllIndCuis_deduplicates_across_exact_and_nlp_indications() {
		SplDrug d = new SplDrug();

		Outcome exactInd = new Outcome();
		Outcome nlpInd = new Outcome();
		exactInd.setIndication(true);
		nlpInd.setIndication(true);

		Atom a1 = atom("A1", "Term 1", "C-IND-1", "PT", "X1", "PX1");
		Atom a2_sameCuiDifferentAui = atom("A2", "Term 1 alt", "C-IND-1", "PT", "X2", "PX2");
		Atom a3 = atom("A3", "Term 2", "C-IND-2", "PT", "X3", "PX3");

		// exact has A1, NLP has A2 (same CUI) and A3
		exactInd.addCode("SRC", a1, new Date());
		nlpInd.addCode("SRC", a2_sameCuiDifferentAui, new Date());
		nlpInd.addCode("SRC", a3, new Date());

		d.setExactMatchIndications(exactInd);
		d.setNlpMatchIndications(nlpInd);

		List<String> cuis = d.getAllIndCuis();

		// Expect only the unique CUIs: C-IND-1, C-IND-2
		assertEquals(Set.of("C-IND-1", "C-IND-2"), new HashSet<>(cuis));
	}

	// ------- getAllAeCuis ----------------------------------------------------

	@Test
	void getAllAeCuis_deduplicates_across_warning_blackbox_and_nlp() {
		SplDrug d = new SplDrug();

		Outcome exWarn = new Outcome();
		exWarn.setWarning(true);
		Outcome exBox = new Outcome();
		exBox.setBlackbox(true);
		Outcome nlpWarn = new Outcome();
		nlpWarn.setWarning(true);
		Outcome nlpBox = new Outcome();
		nlpBox.setBlackbox(true);

		Atom w1 = atom("W1", "Headache", "C-AE-1", "PT", "WX1", "WP1");
		Atom w2_sameCuiDiffAui = atom("W2", "Head pain", "C-AE-1", "PT", "WX2", "WP2");
		Atom b1 = atom("B1", "Anaphylaxis", "C-AE-2", "PT", "BX1", "BP1");

		exWarn.addCode("SRC", w1, new Date());
		nlpWarn.addCode("SRC", w2_sameCuiDiffAui, new Date()); // same CUI as w1
		exBox.addCode("SRC", b1, new Date());

		d.setExactMatchWarnings(exWarn);
		d.setExactMatchBlackbox(exBox);
		d.setNlpMatchWarnings(nlpWarn);
		d.setNlpMatchBlackbox(nlpBox);

		List<String> cuis = d.getAllAeCuis();

		// unique CUIs across all outcomes
		assertEquals(Set.of("C-AE-1", "C-AE-2"), new HashSet<>(cuis));
	}
}
