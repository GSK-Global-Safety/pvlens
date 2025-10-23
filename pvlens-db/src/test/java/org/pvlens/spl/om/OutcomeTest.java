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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.pvlens.spl.umls.Atom;

class OutcomeTest {

	// --- small helper to build a minimal Atom we can use in tests ---
	private static Atom atom(String aui, String term) {
		Atom a = new Atom();
		a.setAui(aui);
		a.setTerm(term);
		a.setCui("CUI-" + aui);
		a.setCode("CODE-" + aui);
		a.setTty("PT");
		a.setPtCode("PT-" + aui);
		a.setDatabaseId(Math.abs(aui.hashCode()));
		return a;
	}

	@Test
	void default_state_is_empty_and_flags_false() {
		Outcome o = new Outcome();
		assertTrue(o.getCodes().isEmpty());
		assertTrue(o.getFirstAdded().isEmpty());
		assertTrue(o.getOutcomeSource().isEmpty());
		assertFalse(o.isBlackbox());
		assertFalse(o.isWarning());
		assertFalse(o.isIndication());
		assertFalse(o.isExactMatch());
	}

	@Test
	void addCode_tracks_code_date_and_source() {
		Outcome o = new Outcome();
		Atom a1 = atom("A1", "Headache");
		Date d1 = new GregorianCalendar(2020, Calendar.JANUARY, 5).getTime();

		o.addCode("ZIP1", a1, d1);

		assertTrue(o.getCodes().contains(a1), "codes should include the added atom");
		assertEquals(d1, o.getFirstAdded().get(a1.getAui()), "firstAdded should store the label date");
		assertTrue(o.getOutcomeSource().containsKey("ZIP1"));
		assertEquals(List.of(a1.getAui()), o.getOutcomeSource().get("ZIP1"));
		assertEquals(List.of("ZIP1"), o.getSources(a1.getAui()));
	}

	@Test
	void updateDateAdded_keeps_earliest_date() {
		Outcome o = new Outcome();
		Atom a1 = atom("A1", "Headache");
		Date later = new GregorianCalendar(2021, Calendar.FEBRUARY, 10).getTime();
		Date earlier = new GregorianCalendar(2019, Calendar.JUNE, 1).getTime();

		// add later first
		o.addCode("ZIP1", a1, later);
		// then try to update with earlier date
		o.updateDateAdded("ZIP1", a1.getAui(), earlier);

		assertEquals(earlier, o.getFirstAdded().get(a1.getAui()), "earliest date should win");
	}

	@Test
	void updateDateAdded_with_null_date_tracks_source_but_does_not_change_firstAdded() {
		Outcome o = new Outcome();
		Atom a1 = atom("A1", "Headache");
		Date d1 = new GregorianCalendar(2020, Calendar.MARCH, 2).getTime();

		o.addCode("ZIP1", a1, d1);
		o.updateDateAdded("ZIP2", a1.getAui(), null); // track source only

		assertEquals(d1, o.getFirstAdded().get(a1.getAui()), "firstAdded should remain unchanged");
		assertTrue(o.getOutcomeSource().containsKey("ZIP2"), "ZIP2 should be tracked as a source");
		assertTrue(o.getSources(a1.getAui()).containsAll(List.of("ZIP1", "ZIP2")));
	}

	@Test
	void addCode_multiple_sources_deduplicates_sources() {
		Outcome o = new Outcome();
		Atom a1 = atom("A1", "Headache");
		Date d = new Date();

		o.addCode(Arrays.asList("Z1", "Z2", "Z1"), a1, d);

		List<String> srcs = o.getSources(a1.getAui());
		assertEquals(2, srcs.size());
		assertTrue(srcs.contains("Z1"));
		assertTrue(srcs.contains("Z2"));
	}

	@Test
	void equals_and_hashCode_ignore_exactMatch_and_consider_flags_plus_codes() {
		Atom a1 = atom("A1", "Headache");
		Atom a2 = atom("A2", "Nausea");
		Date d = new Date();

		Outcome o1 = new Outcome();
		o1.setWarning(true);
		o1.addCode("S", a1, d);
		o1.addCode("S", a2, d);
		o1.setExactMatch(true); // differs

		Outcome o2 = new Outcome();
		o2.setWarning(true);
		o2.addCode("T", a1, d);
		o2.addCode("T", a2, d);
		o2.setExactMatch(false);

		assertEquals(o1, o2, "equal when flags (except exactMatch) and codes match");
		assertEquals(o1.hashCode(), o2.hashCode());

		// flip a classification flag -> should no longer be equal
		o2.setWarning(false);
		assertNotEquals(o1, o2);
	}

	@Test
	void getSources_for_unknown_aui_returns_empty_list() {
		Outcome o = new Outcome();
		assertTrue(o.getSources("NOPE").isEmpty());
	}

	@Test
	void addCode_is_idempotent_on_codes_set() {
		Outcome o = new Outcome();
		Atom a1 = atom("A1", "Headache");
		Date d = new Date();
		o.addCode("Z", a1, d);
		o.addCode("Z", a1, d); // duplicate add

		assertEquals(1, o.getCodes().size(), "codes set should not duplicate atoms");
		assertEquals(1, o.getSources(a1.getAui()).size(), "source list should not duplicate AUI");
	}
}
