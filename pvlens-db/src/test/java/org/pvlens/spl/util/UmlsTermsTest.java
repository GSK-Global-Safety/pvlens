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


import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.pvlens.spl.conf.ConfigLoader;
import org.pvlens.spl.umls.Atom;
import org.pvlens.spl.umls.UmlsLoader;

class UmlsTermsTest {

    @Test
    void createsDeterministicRxnormSql() throws Exception {
        // Temp output dir
        Path tmp = Files.createTempDirectory("umls-terms-test");

        // Mock config to point to temp dir
        ConfigLoader cfg = Mockito.mock(ConfigLoader.class);
        Mockito.when(cfg.getSqlOutputPath()).thenReturn(tmp.toString());

        // Build a small RxNorm map with tricky strings needing escaping
        Map<String, Atom> rx = new LinkedHashMap<>();
        rx.put("A1", atom("A1", "C1", "RXC1", "Acetaminophen \"500mg\"", "PT", 0));
        rx.put("A2", atom("A2", "C2", "RXC2", "Ibuprofen \\ 200mg", "PT", 0));

        UmlsLoader umls = Mockito.mock(UmlsLoader.class);
        Mockito.when(umls.getRxNorm()).thenReturn(rx);

        UmlsTerms terms = new UmlsTerms(cfg, umls, 1000);
        terms.createRxNormTable();

        Path out = tmp.resolve("rxnorm.sql");
        assertTrue(Files.exists(out), "rxnorm.sql should exist");

        String sql = Files.readString(out);
        // Has framing
        assertTrue(sql.contains("SET autocommit"), "Should set autocommit off");
        assertTrue(sql.contains("COMMIT;"), "Should end with COMMIT");

        // Deterministic order by CODE: RXC1 then RXC2
        int p1 = sql.indexOf("Acetaminophen \\\"500mg\\\"");
        int p2 = sql.indexOf("Ibuprofen \\\\ 200mg");
        assertTrue(p1 > 0 && p2 > p1, "Rows must be in deterministic order");

        // IDs assigned and pushed back
        assertEquals(1000, rx.get("A1").getDatabaseId());
        assertEquals(1001, rx.get("A2").getDatabaseId());
    }

    private static Atom atom(String aui, String cui, String code, String term, String tty, int id) {
        Atom a = new Atom();
        a.setAui(aui);
        a.setCui(cui);
        a.setCode(code);
        a.setTerm(term);
        a.setTty(tty);
        a.setDatabaseId(id);
        return a;
    }
}
