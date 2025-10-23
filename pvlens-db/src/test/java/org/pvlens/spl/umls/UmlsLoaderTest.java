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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class UmlsLoaderTest {

	// Create a minimal testable instance:
	// We’ll skip the real constructor because it loads DB + OpenNLP.
	// Instead, we create a subclass whose ctor does nothing heavy.
	private UmlsLoader newInstalledTestLoader() {
		UmlsLoader ldr = UmlsLoader.newTestInstance();
		UmlsLoader.setInstanceForTests(ldr);
		ldr._testSetTokenizer(s -> (s == null || s.isBlank()) ? new String[0] : s.trim().split("\\s+"));
		return ldr;
	}

	@AfterEach
	void cleanup() {
		UmlsLoader.resetInstanceForTests();
	}

	// ---------- text helpers --------------------------------------------------

	@Test
	void umlsCleanText_strips_punct_and_normalizes_case() {
		String in = "  \"Acute-Phase, Reaction!!\"  ";
		String got = UmlsLoader.umlsCleanText(in);
		assertEquals("acute-phase  reaction", got);
	}

	@Test
	void reverseTerm_swaps_known_prefix_suffix_modifiers() {
		assertEquals("increased heart rate", UmlsLoader.reverseTerm("heart rate increased"));
		assertEquals("blood pressure decrease", UmlsLoader.reverseTerm("decrease blood pressure"));
		assertEquals("abnormal liver function", UmlsLoader.reverseTerm("liver function abnormal"));
		// no-op when no keyword
		assertEquals("headache severe", UmlsLoader.reverseTerm("headache severe"));
		// blank input stays blank
		assertEquals("", UmlsLoader.reverseTerm(""));
		assertNull(UmlsLoader.reverseTerm(null));
	}

	// ---------- transformed/stemmed MedDRA maps -------------------------------

	@Test
	void transformed_map_uses_injected_tokenizer_and_sets_max_len() {
		UmlsLoader ldr = newInstalledTestLoader();

		// Seed one MedDRA PT term with a fake AUI
		ldr._testPutMeddraTerm("PT", "Increased blood pressure", "AUI1");

		// Rebuild maps from the seeded data
		ldr._testRebuildTransformedMaps();

		var map = ldr.getTransformedMap();
		assertNotNull(map);

		// token count = 3 with the whitespace tokenizer
		// ("Increased","blood","pressure")
		var byTok = map.get("PT").get(3);
		assertNotNull(byTok); // <== this was failing
		assertTrue(byTok.containsKey("increased blood pressure"));

		assertTrue(ldr.getMaxTokenMatchLength() >= 3);
	}

	@Test
	void stemmed_map_applies_stemming_per_token() {
		UmlsLoader ldr = newInstalledTestLoader();

		// Terms that collapse under stemming ("increasing" -> "increas", etc.)
		ldr._testPutMeddraTerm("PT", "Increasing headache", "AUI2");

		ldr._testRebuildTransformedMaps();

		var map = ldr.getStemmedMap();
		assertNotNull(map);

		// token count = 2 ("Increasing","headache")
		var byTok = map.get("PT").get(2);
		assertNotNull(byTok); // <== this was failing

		// We don't assert exact stem string (depends on Snowball),
		// just that some entry exists for 2 tokens and contains our AUI
		boolean containsAui = byTok.values().stream().anyMatch(list -> list.contains("AUI2"));
		assertTrue(containsAui);
	}

	// ---------- NDC -> ATC lookup --------------------------------------------

	@Test
	void getNdcToAtcCodes_returns_sorted_atoms_and_is_deterministic() {

		UmlsLoader ldr = newInstalledTestLoader();

		// Seed two ATC atoms with different AUIs; we link the same NDC to both
		Atom atc1 = new Atom("ATC_AUI_1", "C1", null, "A01", "alpha blocker", "PT");
		Atom atc2 = new Atom("ATC_AUI_2", "C2", null, "A02", "beta blocker", "PT");

		ldr._testPutAtcAtom("ATC_AUI_2", atc2);
		ldr._testPutAtcAtom("ATC_AUI_1", atc1);

		ldr._testLinkNdcToAtc("12345678901", "ATC_AUI_2");
		ldr._testLinkNdcToAtc("12345678901", "ATC_AUI_1");

		List<Atom> got = ldr.getNdcToAtcCodes("12345678901");

		// Must be sorted by AUI (lexicographic): ATC_AUI_1 then ATC_AUI_2
		assertEquals(2, got.size());
		assertEquals("ATC_AUI_1", got.get(0).getAui());
		assertEquals("ATC_AUI_2", got.get(1).getAui());
	}

	@Test
	void getNdcToAtcCodes_handles_null_or_unknown() {

		UmlsLoader ldr = newInstalledTestLoader();

		assertTrue(ldr.getNdcToAtcCodes(null).isEmpty());
		assertTrue(ldr.getNdcToAtcCodes("not-there").isEmpty());

		// link ndc to unknown AUI (no Atom present) => silently skips
		ldr._testLinkNdcToAtc("000", "UNKNOWN_AUI");
		assertTrue(ldr.getNdcToAtcCodes("000").isEmpty());
	}

}
