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

import java.sql.Connection;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for Db utility using H2 in MySQL mode (no external DB required).
 */
public class DbTest {

    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        // H2 in-memory, MySQL mode for closer SQL behavior
        String url = "jdbc:h2:mem:pvlens;MODE=MySQL;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1";
        conn = Db.getConnection(url, "sa", "", "org.h2.Driver", 0, Duration.ofMillis(1));

        // Schema + seed
        Db.execute(conn,
            "CREATE TABLE meds (" +
                " id INT PRIMARY KEY AUTO_INCREMENT," +
                " code VARCHAR(32) NOT NULL," +
                " name VARCHAR(128) NOT NULL" +
            ")",
            null
        );

        // Insert ~10k rows for streaming checks
        Db.withTransaction(conn, () -> {
            for (int i = 1; i <= 10_000; i++) {
                final int idx = i;
                Db.execute(conn,
                    "INSERT INTO meds(code, name) VALUES(?, ?)",
                    ps -> {
                        ps.setString(1, "C" + idx);
                        ps.setString(2, "NAME_" + idx);
                    }
                );
            }
        });
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
    }

    @Test
    @DisplayName("runQuery: maps a small selection")
    void runQuery_mapsRows() throws Exception {
        var rows = Db.runQuery(conn,
            "SELECT id, code, name FROM meds WHERE id <= ? ORDER BY id",
            ps -> ps.setInt(1, 3),
            rs -> new Row(rs.getInt("id"), rs.getString("code"), rs.getString("name"))
        );

        assertEquals(3, rows.size());
        assertEquals("C1", rows.get(0).code());
        assertEquals("NAME_3", rows.get(2).name());
    }

    
    @Test
    @DisplayName("streamQuery: streams large result set")
    void streamQuery_streamsLargeSet() throws Exception {
        int expected = singleInt("SELECT COUNT(*) FROM meds");  // <- dynamic

        List<Integer> ids = new ArrayList<>();
        Db.streamQuery(conn,
            "SELECT id FROM meds ORDER BY id",
            null,
            rs -> { while (rs.next()) ids.add(rs.getInt(1)); },
            2_000, // fetch size hint
            0
        );

        assertEquals(expected, ids.size());
        assertEquals(1, ids.get(0));
        assertEquals(ids.get(ids.size() - 1).intValue(), expected); // last id equals count for our insert pattern
    }


    @Test
    @DisplayName("streamQuery: preserves caller connection state (readOnly restored)")
    void streamQuery_restoresReadOnly() throws Exception {
        boolean before = conn.isReadOnly();
        Db.streamQuery(conn, "SELECT COUNT(*) FROM meds", null, ResultSet::next);
        boolean after = conn.isReadOnly();
        assertEquals(before, after, "Connection read-only flag should be restored");
    }

    @Test
    @DisplayName("withTransaction: commits and rolls back on error")
    void withTransaction_commitAndRollback() throws Exception {
        int before = singleInt("SELECT COUNT(*) FROM meds WHERE code='COMMIT_TEST'");
        // commit
        Db.withTransaction(conn, () -> {
            Db.execute(conn, "INSERT INTO meds(code, name) VALUES('COMMIT_TEST', 'X')", null);
        });
        int afterCommit = singleInt("SELECT COUNT(*) FROM meds WHERE code='COMMIT_TEST'");
        assertEquals(before + 1, afterCommit);

        int beforeRollback = singleInt("SELECT COUNT(*) FROM meds WHERE code='ROLLBACK_TEST'");
        // rollback
        try {
            Db.withTransaction(conn, () -> {
                Db.execute(conn, "INSERT INTO meds(code, name) VALUES('ROLLBACK_TEST', 'X')", null);
                throw new RuntimeException("boom");
            });
            fail("Expected exception to trigger rollback");
        } catch (RuntimeException expected) {
            // ignore
        }
        int afterRollback = singleInt("SELECT COUNT(*) FROM meds WHERE code='ROLLBACK_TEST'");
        assertEquals(beforeRollback, afterRollback);
    }

    /* ---------------------------- helpers ---------------------------- */

    private int singleInt(String sql) throws Exception {
        var rows = Db.runQuery(conn, sql, null, rs -> rs.getInt(1));
        return rows.get(0);
    }

    private record Row(int id, String code, String name) {}
}
