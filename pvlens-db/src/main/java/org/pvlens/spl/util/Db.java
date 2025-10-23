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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

/**
 * Minimal DB helper with retries, exponential backoff, and streaming support.
 * Designed for MySQL-sized UMLS tables (millions of rows).
 *
 * <p><b>Usage</b>:
 * <pre>
 * try (Connection c = Db.getConnection(jdbcUrl, user, pass, "com.mysql.cj.jdbc.Driver",
 *                                       3, Duration.ofSeconds(1))) {
 *   Db.streamQuery(c, "SELECT id FROM big_table WHERE code = ?",
 *       ps -> ps.setString(1, "X"),
 *       rs -> { while (rs.next()) { /* consume *&#47; } }
 *   );
 * }
 * </pre>
 *
 * <p><b>Notes</b>:
 * <ul>
 *   <li>For MySQL streaming, ensure: {@code useCursorFetch=true}, {@code useServerPrepStmts=true}.</li>
 *   <li>This class does <i>not</i> own the {@link Connection} lifecycle—you open/close it.</li>
 * </ul>
 */
public final class Db {

    private Db() {}

    /* ---------------------------- Functional types ---------------------------- */

    /** Functional interface for setting parameters on a PreparedStatement. */
    @FunctionalInterface
    public interface ParamSetter {
        void accept(PreparedStatement ps) throws Exception;
    }

    /** Functional interface for consuming a ResultSet (streaming). */
    @FunctionalInterface
    public interface ResultSetConsumer {
        void accept(ResultSet rs) throws Exception;
    }

    /** Functional interface for row mapping (non-streaming). */
    @FunctionalInterface
    public interface RowMapper<T> {
        T map(ResultSet rs) throws Exception;
    }

    /* ---------------------------- Connection helpers ---------------------------- */

    /**
     * Get a JDBC connection with simple retry + exponential backoff.
     * If {@code driverClass} is provided, we try to load it once (ignore if missing).
     */
    public static Connection getConnection(String jdbcUrl,
                                           String user,
                                           String pass,
                                           String driverClass,
                                           int maxRetries,
                                           Duration initialBackoff) {
        Objects.requireNonNull(jdbcUrl, "jdbcUrl must not be null");
        Objects.requireNonNull(user, "user must not be null");
        Objects.requireNonNull(pass, "pass must not be null");
        if (maxRetries < 0) throw new IllegalArgumentException("maxRetries must be >= 0");
        if (initialBackoff == null || initialBackoff.isNegative() || initialBackoff.isZero()) {
            initialBackoff = Duration.ofMillis(200);
        }

        if (driverClass != null && !driverClass.isEmpty()) {
            try {
                Class.forName(driverClass);
            } catch (ClassNotFoundException ignored) {
                // rely on service loader / shaded driver
            }
        }

        int attempt = 0;
        long sleepMs = initialBackoff.toMillis();
        while (true) {
            try {
                Properties props = new Properties();
                props.setProperty("user", user);
                props.setProperty("password", pass);

                // MySQL streaming-friendly settings (safe no-ops for H2 / other drivers)
                props.setProperty("useServerPrepStmts", "true");
                props.setProperty("rewriteBatchedStatements", "true");
                props.setProperty("useCursorFetch", "true");

                return DriverManager.getConnection(jdbcUrl, props);
            } catch (Exception ex) {
                attempt++;
                if (attempt > maxRetries) {
                    throw new RuntimeException("DB connection failed after " + maxRetries + " retries: " + ex, ex);
                }
                try {
                    Thread.sleep(sleepMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("DB connection retry interrupted", ie);
                }
                sleepMs = Math.min((long) (sleepMs * 2.0), Duration.ofSeconds(30).toMillis());
            }
        }
    }

    /** Convenience overload with sane defaults. */
    public static Connection getConnection(String jdbcUrl, String user, String pass) {
        // Driver class left null (auto-registered), 3 retries, 250ms backoff
        return getConnection(jdbcUrl, user, pass, null, 3, Duration.ofMillis(250));
    }

    /* ---------------------------- Query helpers ---------------------------- */

    /**
     * Stream a large read-only query. Ensures:
     *  <ul>
     *    <li>forward-only, read-only {@link ResultSet}</li>
     *    <li>positive fetch size (cursor fetch on MySQL if {@code useCursorFetch=true})</li>
     *    <li>optional query timeout</li>
     *  </ul>
     * This method temporarily sets connection read-only and restores it.
     */
    public static void streamQuery(Connection conn,
                                   String sql,
                                   ParamSetter params,
                                   ResultSetConsumer consumer) throws Exception {
        streamQuery(conn, sql, params, consumer, /*fetchSize*/ 5_000, /*queryTimeoutSec*/ 0);
    }

    public static void streamQuery(Connection conn,
                                   String sql,
                                   ParamSetter params,
                                   ResultSetConsumer consumer,
                                   int fetchSize,
                                   int queryTimeoutSeconds) throws Exception {
        Objects.requireNonNull(conn, "conn must not be null");
        Objects.requireNonNull(sql, "sql must not be null");
        Objects.requireNonNull(consumer, "consumer must not be null");
        if (fetchSize <= 0) fetchSize = 5_000;

        boolean previousReadOnly = conn.isReadOnly();
        conn.setReadOnly(true);
        try (PreparedStatement ps = conn.prepareStatement(
                sql,
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY)) {

            ps.setFetchSize(fetchSize);
            if (queryTimeoutSeconds > 0) {
                try {
                    ps.setQueryTimeout(queryTimeoutSeconds);
                } catch (SQLFeatureNotSupportedException ignore) {
                    // Some drivers ignore this
                }
            }
            if (params != null) params.accept(ps);

            try (ResultSet rs = ps.executeQuery()) {
                consumer.accept(rs);
            }
        } finally {
            // Restore previous read-only state to avoid surprising callers
            try {
                conn.setReadOnly(previousReadOnly);
            } catch (SQLException ignored) {}
        }
    }

    /**
     * Run a small query and collect all rows via RowMapper (non-streaming).
     * Prefer {@link #streamQuery} for huge tables.
     */
    public static <T> List<T> runQuery(Connection conn,
                                       String sql,
                                       ParamSetter params,
                                       RowMapper<T> mapper) throws Exception {
        Objects.requireNonNull(conn, "conn must not be null");
        Objects.requireNonNull(sql, "sql must not be null");
        Objects.requireNonNull(mapper, "mapper must not be null");

        List<T> out = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            if (params != null) params.accept(ps);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(mapper.map(rs));
                }
            }
        }
        return out;
    }

    /**
     * Execute DDL/DML that doesn’t return rows (INSERT/UPDATE/DELETE/DDL).
     * Returns the update count. No retries here—callers can add if needed.
     */
    public static int execute(Connection conn, String sql, ParamSetter params) throws Exception {
        Objects.requireNonNull(conn, "conn must not be null");
        Objects.requireNonNull(sql, "sql must not be null");

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            if (params != null) params.accept(ps);
            return ps.executeUpdate();
        }
    }

    /* ---------------------------- Transaction helpers ---------------------------- */

    public static void withTransaction(Connection conn, RunnableEx body) throws Exception {
        Objects.requireNonNull(conn, "conn must not be null");
        boolean prevAuto = conn.getAutoCommit();
        conn.setAutoCommit(false);
        try {
            body.run();
            conn.commit();
        } catch (Exception ex) {
            try { conn.rollback(); } catch (SQLException ignored) {}
            throw ex;
        } finally {
            try { conn.setAutoCommit(prevAuto); } catch (SQLException ignored) {}
        }
    }

    @FunctionalInterface
    public interface RunnableEx {
        void run() throws Exception;
    }
}
