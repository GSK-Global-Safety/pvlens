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

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

class AtomTest {

    private static Atom sample() {
        Atom a = new Atom("AUI123", "CUI999", "PT0001", "MDRA1234", "Severe Rash", "PT");
        a.setDatabaseId(42);
        a.setIsPref("Y");
        a.setSab("MDR");
        return a;
    }

    // ---------------- isMatch -------------------------------------------------

    @Test
    void isMatch_all_fields_match_with_case_insensitive_term() {
        Atom a = sample();
        assertTrue(a.isMatch("CUI999", "PT", " severe rash "));
    }

    @Test
    void isMatch_cui_or_tty_mismatch_returns_false() {
        Atom a = sample();
        assertFalse(a.isMatch("CUI998", "PT", "Severe Rash"));
        assertFalse(a.isMatch("CUI999", "LLT", "Severe Rash"));
    }

    @Test
    void isMatch_null_term_treated_as_empty_string() {
        Atom a = sample();
        a.setTerm(""); // atom term empty
        assertTrue(a.isMatch("CUI999", "PT", null));
        assertFalse(a.isMatch("CUI999", "PT", "non-empty"));
    }

    // ---------------- getOutputRecord ----------------------------------------

    @Test
    void getOutputRecord_layout_and_trailing_delimiter() {
        Atom a = sample();
        String rec = a.getOutputRecord();

        // Expect 15 fields separated by | and a trailing delimiter ( => 15 delimiters)
        // Split preserving empties: use -1 limit.
        String[] parts = rec.split("\\|", -1);

        // There are 15 fields *plus* an empty field after the trailing delimiter,
        // so parts.length should be 16 with the last being "".
        assertEquals(16, parts.length, "MRCONSO-like output should end with trailing delimiter");
        assertEquals("", parts[15]);

        // Key positions
        assertEquals("CUI999", parts[0]);   // CUI
        assertEquals("AUI123", parts[7]);   // AUI
        assertEquals("PT0001", parts[10]);  // PT_CODE
        assertEquals("MDR",    parts[11]);  // SAB is hard-coded
        assertEquals("PT",     parts[12]);  // TTY
        assertEquals("MDRA1234", parts[13]);// CODE
        assertEquals("Severe Rash", parts[14]); // TERM
    }

    @Test
    void getOutputRecord_handles_nulls_as_empty_fields() {
        Atom a = new Atom();
        a.setCui(null);
        a.setAui(null);
        a.setPtCode(null);
        a.setTty(null);
        a.setCode(null);
        a.setTerm(null);

        String rec = a.getOutputRecord();
        String[] parts = rec.split("\\|", -1);

        // Still 16 tokens (trailing empty), and SAB=MDR at index 11
        assertEquals(16, parts.length);
        assertEquals("MDR", parts[11]);
        // all key fields empty
        assertEquals("", parts[0]);
        assertEquals("", parts[7]);
        assertEquals("", parts[10]);
        assertEquals("", parts[12]);
        assertEquals("", parts[13]);
        assertEquals("", parts[14]);
    }

    // ---------------- CSV exports --------------------------------------------

    @Test
    void toMdrCsvString_default_delimiter_and_values() {
        Atom a = sample();
        String s = a.toMdrCsvString(null); // default to "|"
        String[] parts = s.split("\\|", -1);
        assertArrayEquals(
            new String[] {"42","AUI123","CUI999","MDRA1234","PT0001","Severe Rash","PT"},
            parts
        );
    }

    @Test
    void toMdrCsvString_custom_delimiter_and_nulls() {
        Atom a = new Atom();
        a.setDatabaseId(-1);
        a.setAui("A");
        a.setCui(null);
        a.setCode("X");
        a.setPtCode(null);
        a.setTerm(null);
        a.setTty("LLT");

        String s = a.toMdrCsvString(",");
        assertEquals("-1,A,,X,,," + "LLT", s); // middle empties preserved
    }

    @Test
    void toCsvString_default_delimiter_and_values() {
        Atom a = sample();
        String s = a.toCsvString(null);
        String[] parts = s.split("\\|", -1);
        assertArrayEquals(
            new String[] {"42","AUI123","CUI999","MDRA1234","Severe Rash","PT"},
            parts
        );
    }

    @Test
    void toCsvString_custom_delimiter_and_nulls() {
        Atom a = new Atom();
        a.setDatabaseId(7);
        a.setAui(null);
        a.setCui("C1");
        a.setCode(null);
        a.setTerm(" t ");
        a.setTty(null);

        String s = a.toCsvString(",");
        assertEquals("7,,C1,, t ,", s);
    }

    // ---------------- defensive setters --------------------------------------

    @Test
    void defensive_setters_for_parents_children_ptr() {
        Atom a = new Atom();

        // parents/children accept null and copy values
        a.setParents(null);
        a.setChildren(null);
        assertNotNull(a.getParents());
        assertNotNull(a.getChildren());
        assertTrue(a.getParents().isEmpty());
        assertTrue(a.getChildren().isEmpty());

        List<Long> p = Arrays.asList(1L, 2L);
        a.setParents(p);
        p.set(0, 99L); // mutate original
        assertEquals(Arrays.asList(1L, 2L), a.getParents(), "Should be defensively copied");

        List<Long> c = Arrays.asList(3L);
        a.setChildren(c);
        c.set(0, 88L);
        assertEquals(Arrays.asList(3L), a.getChildren(), "Should be defensively copied");

        // ptr is copied defensively too
        String[] path = new String[] {"A","B"};
        a.setPtr(path);
        path[0] = "Z";
        assertArrayEquals(new String[] {"A","B"}, a.getPtr());
    }
}
