package org.pvlens.spl.processing.persist;

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

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Thin wrapper around multiple {@link PrintWriter} instances that:
 * <ul>
 *   <li>Emits a transaction preamble ("SET autocommit = OFF;") to each stream,</li>
 *   <li>Provides keyed access via {@link #get(String)},</li>
 *   <li>Flushes and closes all writers with a trailing "COMMIT;".</li>
 * </ul>
 *
 * <p>Note: This class does not manage file creation; callers should open the
 * {@code PrintWriter}s with desired encodings and pass them in.</p>
 */
public class SqlWriters implements AutoCloseable {

    private static final String SQL_COMMIT_OFF = "SET autocommit = OFF;";
    private static final String SQL_COMMIT_ON  = "COMMIT;";

    private final Map<String, PrintWriter> writers = new HashMap<>();

    public SqlWriters(Map<String, PrintWriter> w) {
        Objects.requireNonNull(w, "writers");
        writers.putAll(w);
        writers.values().forEach(pw -> pw.println(SQL_COMMIT_OFF));
    }

    /** Returns the writer for a given key (e.g., "PRODUCT", "AE"). */
    public PrintWriter get(String key) {
        return writers.get(key);
    }

    /** Flush all writers without closing them. */
    public void flushAll() {
        writers.values().forEach(PrintWriter::flush);
    }

    /** Print "COMMIT;", flush, and close all writers. Safe to call once at end of run. */
    @Override
    public void close() {
        writers.values().forEach(pw -> {
            pw.println(SQL_COMMIT_ON);
            pw.flush();
            pw.close();
        });
    }
}
