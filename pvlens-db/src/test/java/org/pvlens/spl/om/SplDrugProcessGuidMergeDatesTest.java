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

import static org.junit.jupiter.api.Assertions.*;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.junit.jupiter.api.Test;
import org.pvlens.spl.umls.Atom;

/**
 * Tests that simulate the "per XML → tmpSpl → mergeProductGroup" flow used by
 * processGuid(). The goals: - After merging multiple tmpSpls (in any order),
 * the earliest firstAdded date per CUI is preserved across the relevant
 * buckets. - No shared mutable structures between clones (defensive copies). -
 * Merge is idempotent.
 */
public class SplDrugProcessGuidMergeDatesTest {

	private static final SimpleDateFormat DF = new SimpleDateFormat("yyyy-MM-dd");

	private static Date d(String ymd) {
		try {
			return DF.parse(ymd);
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
	}

	private static Atom atom(String cui, String aui, String ptCode) {
		Atom a = new Atom();
		a.setCui(cui);
		a.setAui(aui);
		a.setCode(ptCode);
		a.setPtCode(ptCode);
		return a;
	}

	private static void put(Outcome out, Atom a, Date firstAdded, String sourceGuid) {
		// minimal “addCode” equivalent: inject into codes + firstAdded + source map
		out.getCodes().add(a);
		if (firstAdded != null) {
			out.getFirstAdded().put(a.getAui(), firstAdded);
		}
		if (sourceGuid != null) {
			out.getOutcomeSource().computeIfAbsent(sourceGuid, k -> new ArrayList<>()).add(a.getAui());
		}
	}

	private static SplDrug makeBase(String guid, String xmlPath, int sourceType) {
		SplDrug s = new SplDrug();
		s.setGuid(guid);
		s.setSourceType(sourceType);
		s.addXmlFile(xmlPath, false); // ensure we simulate per-XML set
		return s;
	}

	private static SplDrug makeTmpFrom(SplDrug base) {
		// mirrors processGuid(): SplDrug tmpSpl = spl.copySplDrug();
		return base.copySplDrug();
	}

	@Test
	void indications_earliest_date_wins_across_multiple_tmp_merges_any_order() {
		// Base product
		SplDrug spl = makeBase("G-1", "/path/prescription/one.xml", 1);

		// First tmp (later date)
		SplDrug tmp1 = makeTmpFrom(spl);
		Atom aInd = atom("C001", "A001", "P001");
		tmp1.getExactMatchIndications().setExactMatch(true);
		put(tmp1.getExactMatchIndications(), aInd, d("2020-06-01"), spl.getGuid());

		// Merge later-first
		boolean m1 = spl.mergeProductGroup(tmp1);
		assertTrue(m1, "Expected first merge to succeed");

		// Second tmp (earlier date for same CUI but different AUI/bucket)
		SplDrug tmp2 = makeTmpFrom(spl);
		Atom aIndNlp = atom("C001", "A002", "P001");
		tmp2.getNlpMatchIndications().setExactMatch(false);
		put(tmp2.getNlpMatchIndications(), aIndNlp, d("2018-03-10"), spl.getGuid());

		// Merge earlier-second
		boolean m2 = spl.mergeProductGroup(tmp2);
		assertTrue(m2, "Expected second merge to succeed");

		// After both merges, the earliest date should propagate to both AUIs and both
		// buckets
		Date min = d("2018-03-10");
		assertEquals(min, spl.getExactMatchIndications().getFirstAdded().get("A001"));
		assertEquals(min, spl.getNlpMatchIndications().getFirstAdded().get("A002"));
	}

	@Test
	void warnings_blackbox_earliest_date_wins_across_all_warning_like_buckets() {
		SplDrug spl = makeBase("G-2", "/path/prescription/two.xml", 1);

		// tmp with various dates across warning buckets
		SplDrug t1 = makeTmpFrom(spl);
		Atom wExact = atom("C100", "W001", "PW1");
		put(t1.getExactMatchWarnings(), wExact, d("2019-01-15"), spl.getGuid());

		SplDrug t2 = makeTmpFrom(spl);
		Atom wNlp = atom("C100", "W002", "PW1");
		put(t2.getNlpMatchWarnings(), wNlp, d("2018-12-31"), spl.getGuid()); // earliest

		SplDrug t3 = makeTmpFrom(spl);
		Atom bbExact = atom("C100", "B001", "PB1");
		put(t3.getExactMatchBlackbox(), bbExact, d("2019-05-01"), spl.getGuid());

		SplDrug t4 = makeTmpFrom(spl);
		Atom bbNlp = atom("C100", "B002", "PB1");
		put(t4.getNlpMatchBlackbox(), bbNlp, d("2019-07-20"), spl.getGuid());

		assertTrue(spl.mergeProductGroup(t1));
		assertTrue(spl.mergeProductGroup(t2));
		assertTrue(spl.mergeProductGroup(t3));
		assertTrue(spl.mergeProductGroup(t4));

		Date min = d("2018-12-31");
		assertEquals(min, spl.getExactMatchWarnings().getFirstAdded().get("W001"));
		assertEquals(min, spl.getNlpMatchWarnings().getFirstAdded().get("W002"));
		assertEquals(min, spl.getExactMatchBlackbox().getFirstAdded().get("B001"));
		assertEquals(min, spl.getNlpMatchBlackbox().getFirstAdded().get("B002"));
	}

	@Test
	void merges_out_of_order_still_produce_minima() {
		SplDrug spl = makeBase("G-3", "/path/otc/one.xml", 2);

		// Merge earlier first
		SplDrug tEarly = makeTmpFrom(spl);
		Atom a = atom("C777", "A777", "PT7");
		put(tEarly.getExactMatchIndications(), a, d("2010-02-02"), spl.getGuid());
		assertTrue(spl.mergeProductGroup(tEarly));

		// Now merge a later date on a different AUI/bucket for same CUI
		SplDrug tLate = makeTmpFrom(spl);
		Atom b = atom("C777", "B777", "PT7");
		put(tLate.getNlpMatchIndications(), b, d("2012-09-15"), spl.getGuid());
		assertTrue(spl.mergeProductGroup(tLate));

		// Earliest should stand for both AUIs
		Date min = d("2010-02-02");
		assertEquals(min, spl.getExactMatchIndications().getFirstAdded().get("A777"));
		assertEquals(min, spl.getNlpMatchIndications().getFirstAdded().get("B777"));
	}

	@Test
	void copy_is_defensive_and_does_not_share_maps() {
		SplDrug base = makeBase("G-X", "/path/prescription/x.xml", 1);
		SplDrug copy = base.copySplDrug();

		assertNotSame(base.getXmlFiles(), copy.getXmlFiles(), "xmlFiles should be deep-copied");
		assertNotSame(base.getDrugProduct(), copy.getDrugProduct(), "drugProduct should be deep-copied");
		// also mergedGuidXmlPairs/guidApprovalDate/guidNda defensive copies
		assertNotSame(base.getMergedGuidXmlPairs(), copy.getMergedGuidXmlPairs());
		assertNotSame(base.getGuidApprovalDate(), copy.getGuidApprovalDate());
		assertNotSame(base.getGuidNda(), copy.getGuidNda());

		// Mutations on copy do not affect base
		copy.addXmlFile("/another.xml", false);
		assertFalse(base.getXmlFiles().containsKey("/another.xml"));
	}

	@Test
	void merge_is_idempotent_for_same_tmp_data() {
		SplDrug spl = makeBase("G-4", "/path/other/a.xml", 3);

		SplDrug t = makeTmpFrom(spl);
		Atom a = atom("C900", "A900", "PX");
		put(t.getExactMatchWarnings(), a, d("2021-01-01"), spl.getGuid());

		assertTrue(spl.mergeProductGroup(t));
		Map<String, Date> snap = new HashMap<>(spl.getExactMatchWarnings().getFirstAdded());

		// Merge again with the same tmp
		assertTrue(spl.mergeProductGroup(t));
		assertEquals(snap, spl.getExactMatchWarnings().getFirstAdded(), "Repeated merges should not change minima");
	}

	@Test
	void different_cuis_do_not_cross_contaminate() {
		SplDrug spl = makeBase("G-5", "/path/prescription/z.xml", 1);

		SplDrug t1 = makeTmpFrom(spl);
		Atom a1 = atom("C111", "A111", "P1");
		put(t1.getExactMatchIndications(), a1, d("2015-01-01"), spl.getGuid());

		SplDrug t2 = makeTmpFrom(spl);
		Atom a2 = atom("C222", "A222", "P2");
		put(t2.getNlpMatchIndications(), a2, d("2010-01-01"), spl.getGuid());

		assertTrue(spl.mergeProductGroup(t1));
		assertTrue(spl.mergeProductGroup(t2));

		// Ensure each CUI keeps its own min (no crossover)
		assertEquals(d("2015-01-01"), spl.getExactMatchIndications().getFirstAdded().get("A111"));
		assertEquals(d("2010-01-01"), spl.getNlpMatchIndications().getFirstAdded().get("A222"));
	}
}
