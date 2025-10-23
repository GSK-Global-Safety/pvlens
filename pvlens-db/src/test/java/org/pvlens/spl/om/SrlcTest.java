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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Date;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.pvlens.spl.umls.Atom;

class SrlcTest {

	// -------- helpers --------------------------------------------------------

	private static Atom atom(String aui, String cui) {
		Atom a = new Atom();
		a.setAui(aui);
		a.setCui(cui);
		a.setTerm("T-" + aui);
		a.setTty("PT");
		a.setCode("CODE-" + aui);
		a.setPtCode("PT-" + aui);
		a.setDatabaseId(Math.abs(aui.hashCode()));
		return a;
	}

	private static void add(Outcome out, String src, Atom a, Date d) {
		out.addCode(src, a, d);
	}

	// -------- construction / defaults ---------------------------------------

	@Test
	void default_ctor_initializes_outcomes_and_flags() {
		Srlc s = new Srlc();
		assertNotNull(s.getExactAeMatch());
		assertNotNull(s.getNlpAeMatch());
		assertNotNull(s.getExactBlackboxMatch());
		assertNotNull(s.getNlpBlackboxMatch());

		assertTrue(s.getExactAeMatch().getCodes().isEmpty());
		assertTrue(s.getNlpAeMatch().getCodes().isEmpty());
		assertTrue(s.getExactBlackboxMatch().getCodes().isEmpty());
		assertTrue(s.getNlpBlackboxMatch().getCodes().isEmpty());

		assertFalse(s.hasCodes());
	}

	// -------- getDrugId() ----------------------------------------------------

	@Test
	void getDrugId_extracts_from_query_param_drug_id() {
		Srlc s = new Srlc();
		s.setUrl("https://example.com/srlc?foo=bar&drug_id=12345&x=y");
		assertEquals(12345, s.getDrugId());
	}

	@Test
	void getDrugId_extracts_from_id_aliases_and_legacy_position() {
		Srlc a = new Srlc();
		a.setUrl("https://host/path?drugid=54321"); // alias
		assertEquals(54321, a.getDrugId());

		Srlc b = new Srlc();
		b.setUrl("https://host/path?id=9876&z=z"); // alias
		assertEquals(9876, b.getDrugId());

		// legacy: assume second pair if no known key
		Srlc c = new Srlc();
		c.setUrl("https://host/path?first=x&second=24680&third=y");
		assertEquals(24680, c.getDrugId());
	}

	@Test
	void getDrugId_returns_minus_one_on_missing_or_bad_url() {
		Srlc s1 = new Srlc();
		s1.setUrl(null);
		assertEquals(-1, s1.getDrugId());

		Srlc s2 = new Srlc();
		s2.setUrl("https://host/path"); // no query
		assertEquals(-1, s2.getDrugId());

		Srlc s3 = new Srlc();
		s3.setUrl("https://host/path?drug_id=notANumber");
		assertEquals(-1, s3.getDrugId());
	}

	// -------- hasCodes() -----------------------------------------------------

	@Test
	void hasCodes_true_when_any_outcome_has_codes() {
		Srlc s = new Srlc();

		assertFalse(s.hasCodes());

		Atom a = atom("A1", "C1");
		add(s.getExactAeMatch(), "SRC", a, new Date());
		assertTrue(s.hasCodes());

		// add elsewhere just to be sure
		Atom b = atom("B1", "C2");
		add(s.getNlpBlackboxMatch(), "SRC", b, new Date());
		assertTrue(s.hasCodes());
	}

	// -------- resolveLabeledEvents() & cleanupFirstAdded() -------------------

	@Test
	void resolveLabeledEvents_drops_exact_from_nlp_and_prunes_firstAdded() {
		Srlc s = new Srlc();

		// same atom instance in exact + NLP -> NLP should drop it
		Atom duplicate = atom("DUP", "C-DUP");
		Date dExact = new Date(1_700_000_000_000L);
		Date dNlp = new Date(1_710_000_000_000L);

		add(s.getExactAeMatch(), "SRC-EXACT", duplicate, dExact);
		add(s.getNlpAeMatch(), "SRC-NLP", duplicate, dNlp);

		Atom boxExact = atom("BOX-E", "C-BOX");
		Atom boxOnlyNlp = atom("BOX-N", "C-ONLY-NLP");
		Date dBoxExact = new Date(1_720_000_000_000L);
		Date dBoxOnlyNlp = new Date(1_730_000_000_000L);

		add(s.getExactBlackboxMatch(), "SRC-BE", boxExact, dBoxExact);
		add(s.getNlpBlackboxMatch(), "SRC-BN", boxOnlyNlp, dBoxOnlyNlp);
		add(s.getNlpBlackboxMatch(), "SRC-BN", boxExact, dBoxOnlyNlp); // duplicate to be removed

		// preconditions
		assertTrue(s.getNlpAeMatch().getCodes().contains(duplicate));
		assertTrue(s.getNlpBlackboxMatch().getCodes().contains(boxExact));
		assertTrue(s.getNlpBlackboxMatch().getCodes().contains(boxOnlyNlp));
		assertTrue(s.getNlpAeMatch().getFirstAdded().containsKey("DUP"));
		assertTrue(s.getNlpBlackboxMatch().getFirstAdded().containsKey("BOX-E"));
		assertTrue(s.getNlpBlackboxMatch().getFirstAdded().containsKey("BOX-N"));

		// resolve
		s.resolveLabeledEvents();

		// NLP AE lost the duplicate present in exact AE
		assertFalse(s.getNlpAeMatch().getCodes().contains(duplicate));
		// and firstAdded pruned for it
		assertFalse(s.getNlpAeMatch().getFirstAdded().containsKey("DUP"));

		// NLP Blackbox lost the duplicate present in exact Blackbox
		assertFalse(s.getNlpBlackboxMatch().getCodes().contains(boxExact));
		// and firstAdded pruned for it
		assertFalse(s.getNlpBlackboxMatch().getFirstAdded().containsKey("BOX-E"));

		// NLP Blackbox still has the non-duplicate
		assertTrue(s.getNlpBlackboxMatch().getCodes().contains(boxOnlyNlp));
		assertTrue(s.getNlpBlackboxMatch().getFirstAdded().containsKey("BOX-N"));

		// Exact outcomes untouched
		assertTrue(s.getExactAeMatch().getCodes().contains(duplicate));
		assertTrue(s.getExactBlackboxMatch().getCodes().contains(boxExact));

		// Sources are still tracked for kept items
		List<String> keptSources = s.getNlpBlackboxMatch().getOutcomeSource().get("SRC-BN");
		assertNotNull(keptSources);
		assertTrue(keptSources.contains("BOX-N"));
	}
}
