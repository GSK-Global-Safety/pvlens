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


import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pvlens.spl.umls.Atom;

/**
 * Unit tests for SplDrug.reconcileLabelFirstAddedDatesWithinDrug()
 *
 * These tests validate that, for each CUI: - Indications: earliest date across
 * (exact + NLP) indications is propagated back to both maps. -
 * Warnings/BlackBox: earliest date across (exact + NLP warnings + exact + NLP
 * black box) is propagated back. - Null dates do not overwrite a non-null
 * earlier date. - Different CUIs remain independent. - Method is idempotent.
 */
public class SplDrugReconcileDatesTest {

	private static final SimpleDateFormat DF = new SimpleDateFormat("yyyy-MM-dd");

	private static Date d(String ymd) {
		try {
			return DF.parse(ymd);
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
	}

	private static Atom atom(String cui, String aui) {
		// If Atom has a builder/constructor, use that. Otherwise adapt here.
		Atom a = new Atom();
		a.setCui(cui);
		a.setAui(aui);
		return a;
	}

	private SplDrug drug;

	@BeforeEach
	void setUp() {
		drug = new SplDrug();
		// Ensure empty collections/maps are initialized (if the class doesn’t already)
		// e.g., drug.getExactMatchIndications().initIfNeeded(); — not needed in most
		// implementations
	}

	// ---------- Helpers to insert test data into buckets ----------

	private static void putIndExact(SplDrug sd, Atom a, Date when) {
		sd.getExactMatchIndications().getCodes().add(a);
		if (when != null)
			sd.getExactMatchIndications().getFirstAdded().put(a.getAui(), when);
	}

	private static void putIndNlp(SplDrug sd, Atom a, Date when) {
		sd.getNlpMatchIndications().getCodes().add(a);
		if (when != null)
			sd.getNlpMatchIndications().getFirstAdded().put(a.getAui(), when);
	}

	private static void putWarnExact(SplDrug sd, Atom a, Date when) {
		sd.getExactMatchWarnings().getCodes().add(a);
		if (when != null)
			sd.getExactMatchWarnings().getFirstAdded().put(a.getAui(), when);
	}

	private static void putWarnNlp(SplDrug sd, Atom a, Date when) {
		sd.getNlpMatchWarnings().getCodes().add(a);
		if (when != null)
			sd.getNlpMatchWarnings().getFirstAdded().put(a.getAui(), when);
	}

	private static void putBoxExact(SplDrug sd, Atom a, Date when) {
		sd.getExactMatchBlackbox().getCodes().add(a);
		if (when != null)
			sd.getExactMatchBlackbox().getFirstAdded().put(a.getAui(), when);
	}

	private static void putBoxNlp(SplDrug sd, Atom a, Date when) {
		sd.getNlpMatchBlackbox().getCodes().add(a);
		if (when != null)
			sd.getNlpMatchBlackbox().getFirstAdded().put(a.getAui(), when);
	}

	// ------------------- Tests -------------------

	@Test
	void indications_minimum_propagates_across_exact_and_nlp() {
		// Same CUI across two AUIs, different buckets/dates
		Atom indExact = atom("C001", "A001");
		Atom indNlp = atom("C001", "A002");

		putIndExact(drug, indExact, d("2018-05-10")); // later
		putIndNlp(drug, indNlp, d("2017-03-01")); // earliest

		drug.reconcileLabelFirstAddedDatesWithinDrug();

		Date min = d("2017-03-01");
		assertEquals(min, drug.getExactMatchIndications().getFirstAdded().get("A001"));
		assertEquals(min, drug.getNlpMatchIndications().getFirstAdded().get("A002"));
	}

	@Test
	void warnings_and_blackbox_share_minimum_across_all_warning_buckets() {
		// Same CUI spread across exact warning, nlp warning, and blackbox with
		// different dates
		Atom warnExact = atom("C002", "W001");
		Atom warnNlp = atom("C002", "W002");
		Atom boxExact = atom("C002", "B001");
		Atom boxNlp = atom("C002", "B002");

		putWarnExact(drug, warnExact, d("2020-01-01"));
		putWarnNlp(drug, warnNlp, d("2019-12-01")); // earliest
		putBoxExact(drug, boxExact, d("2020-02-01"));
		putBoxNlp(drug, boxNlp, d("2020-03-15"));

		drug.reconcileLabelFirstAddedDatesWithinDrug();

		Date min = d("2019-12-01");
		assertEquals(min, drug.getExactMatchWarnings().getFirstAdded().get("W001"));
		assertEquals(min, drug.getNlpMatchWarnings().getFirstAdded().get("W002"));
		assertEquals(min, drug.getExactMatchBlackbox().getFirstAdded().get("B001"));
		assertEquals(min, drug.getNlpMatchBlackbox().getFirstAdded().get("B002"));
	}

	@Test
	void different_cuis_do_not_cross_contaminate() {
		// C003 gets an early date in NLP warnings; C004 has a later date in exact
		// warnings
		Atom a1 = atom("C003", "X1");
		Atom a2 = atom("C004", "X2");

		putWarnNlp(drug, a1, d("2016-06-06")); // earliest for C003
		putWarnExact(drug, a2, d("2018-08-08")); // only for C004

		drug.reconcileLabelFirstAddedDatesWithinDrug();

		assertEquals(d("2016-06-06"), drug.getNlpMatchWarnings().getFirstAdded().get("X1"));
		assertEquals(d("2018-08-08"), drug.getExactMatchWarnings().getFirstAdded().get("X2"));
		// Ensure maps for the other CUI don't get touched with the wrong minimum
		assertFalse(drug.getExactMatchWarnings().getFirstAdded().containsKey("X1"));
		assertFalse(drug.getNlpMatchWarnings().getFirstAdded().containsKey("X2"));
	}

	@Test
	void null_dates_are_ignored_and_do_not_override_real_dates() {
		Atom a1 = atom("C005", "N1");
		Atom a2 = atom("C005", "N2");

		putIndExact(drug, a1, null); // unknown date
		putIndNlp(drug, a2, d("2015-01-15")); // real earliest

		drug.reconcileLabelFirstAddedDatesWithinDrug();

		// N2 keeps its real date
		assertEquals(d("2015-01-15"), drug.getNlpMatchIndications().getFirstAdded().get("N2"));
		// N1 (null before) is filled with the min
		assertEquals(d("2015-01-15"), drug.getExactMatchIndications().getFirstAdded().get("N1"));
	}

	@Test
	void idempotent_rerunning_does_not_change_results() {
		Atom a1 = atom("C006", "Z1");
		Atom a2 = atom("C006", "Z2");
		putWarnExact(drug, a1, d("2017-07-07"));
		putBoxExact(drug, a2, d("2017-06-01")); // earliest

		drug.reconcileLabelFirstAddedDatesWithinDrug();

		Map<String, Date> snapshot1 = new HashMap<>(drug.getExactMatchWarnings().getFirstAdded());
		Map<String, Date> snapshot2 = new HashMap<>(drug.getExactMatchBlackbox().getFirstAdded());

		drug.reconcileLabelFirstAddedDatesWithinDrug(); // run again

		assertEquals(snapshot1, drug.getExactMatchWarnings().getFirstAdded());
		assertEquals(snapshot2, drug.getExactMatchBlackbox().getFirstAdded());
	}

	@Test
	void buckets_missing_or_empty_are_handled_gracefully() {
		// If your SplDrug sometimes has empty code sets or maps, the method should be a
		// no-op
		// Create a fresh drug with no codes added:
		SplDrug empty = new SplDrug();
		assertDoesNotThrow(empty::reconcileLabelFirstAddedDatesWithinDrug);
	}
}