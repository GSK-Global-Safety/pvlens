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

import java.io.PrintStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/**
 * Minimal, dependency-free logger for PVLens.
 *
 * <p>Features:</p>
 * <ul>
 *   <li>Log levels (TRACE, DEBUG, INFO, WARN, ERROR)</li>
 *   <li>Timestamp + thread name in each line</li>
 *   <li>Thread-safe output</li>
 *   <li>Configuration via system properties:
 *     <ul>
 *       <li><b>pvlens.log.level</b> – minimum level to print (default: INFO)</li>
 *       <li><b>pvlens.log.datetime</b> – pattern (default: yyyy-MM-dd HH:mm:ss)</li>
 *     </ul>
 *   </li>
 *   <li>Convenience methods; {@code log(String)} kept for compatibility</li>
 * </ul>
 */
public final class Logger {

    /** Log levels in increasing order of severity. */
    public enum Level {
        TRACE, DEBUG, INFO, WARN, ERROR;

        static Level parse(String s, Level fallback) {
            if (s == null) return fallback;
            try {
                return Level.valueOf(s.trim().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException ex) {
                return fallback;
            }
        }
    }

    // ---- Configuration (read once at class load) ----
    private static final Level MIN_LEVEL =
            Level.parse(System.getProperty("pvlens.log.level"), Level.INFO);

    private static final DateTimeFormatter TS =
            DateTimeFormatter.ofPattern(
                    System.getProperty("pvlens.log.datetime", "yyyy-MM-dd HH:mm:ss")
            );

    private Logger() {}

    // ---- Public API (compat + convenience) ----

    /** Backward-compatible alias for {@link #info(String, Object...)}. */
    public static void log(String text) {
        info(text);
    }

    public static void trace(String msg, Object... args) { log(Level.TRACE, null, msg, args); }
    public static void debug(String msg, Object... args) { log(Level.DEBUG, null, msg, args); }
    public static void info (String msg, Object... args) { log(Level.INFO , null, msg, args); }
    public static void warn (String msg, Object... args) { log(Level.WARN , null, msg, args); }
    public static void error(String msg, Object... args) { log(Level.ERROR, null, msg, args); }

    public static void trace(String msg, Throwable t, Object... args) { log(Level.TRACE, t, msg, args); }
    public static void debug(String msg, Throwable t, Object... args) { log(Level.DEBUG, t, msg, args); }
    public static void info (String msg, Throwable t, Object... args) { log(Level.INFO , t, msg, args); }
    public static void warn (String msg, Throwable t, Object... args) { log(Level.WARN , t, msg, args); }
    public static void error(String msg, Throwable t, Object... args) { log(Level.ERROR, t, msg, args); }

    // ---- Core implementation ----

    private static void log(Level level, Throwable t, String msg, Object... args) {
        if (level.ordinal() < MIN_LEVEL.ordinal()) return;

        final String ts = LocalDateTime.now().format(TS);
        final String thread = Thread.currentThread().getName();
        final String body = safeFormat(msg, args);

        // INFO and below -> stdout; WARN/ERROR -> stderr
        final PrintStream out = (level.ordinal() >= Level.WARN.ordinal()) ? System.err : System.out;

        synchronized (Logger.class) {
            out.println("[" + ts + "] [" + thread + "] " + level + " " + body);
            if (t != null) {
                t.printStackTrace(out);
            }
        }
    }

    /**
     * Simple, safe string formatter:
     * replaces each "{}" with the stringified next argument.
     * If counts mismatch, extra args are appended.
     */
    private static String safeFormat(String template, Object... args) {
        if (template == null) return "null";
        if (args == null || args.length == 0) return template;

        StringBuilder sb = new StringBuilder(template.length() + args.length * 8);
        int argIdx = 0;
        for (int i = 0; i < template.length(); i++) {
            char c = template.charAt(i);
            if (c == '{' && i + 1 < template.length() && template.charAt(i + 1) == '}' && argIdx < args.length) {
                sb.append(String.valueOf(args[argIdx++]));
                i++; // skip '}'
            } else {
                sb.append(c);
            }
        }
        while (argIdx < args.length) {
            sb.append(' ').append(String.valueOf(args[argIdx++]));
        }
        return sb.toString();
    }
}
