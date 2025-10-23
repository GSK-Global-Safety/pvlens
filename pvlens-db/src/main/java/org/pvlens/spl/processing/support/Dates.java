package org.pvlens.spl.processing.support;

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

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Small date utilities for parsing/formatting SPL timestamps.
 * <p>Note: {@link SimpleDateFormat} is not thread-safe; callers must provide a
 * thread-confined instance when using {@link #parse(SimpleDateFormat, String)}
 * and {@link #fmt(SimpleDateFormat, Date)}.</p>
 */
public final class Dates {

	private Dates() { /* no instances */ }

	/** Database timestamp pattern used throughout the pipeline (UTC). */
	public static final String DB_FMT = "yyyy-MM-dd HH:mm:ss";

	/**
	 * Parse a date with the given formatter; returns {@code null} on failure.
	 */
	public static Date parse(SimpleDateFormat fmt, String s) {
		try {
			return fmt.parse(s);
		} catch (Exception e) {
			return null;
		}
	}

	/**
	 * Format a date with the given formatter; returns {@code null} if {@code d} is null.
	 */
	public static String fmt(SimpleDateFormat fmt, Date d) {
		return (d == null) ? null : fmt.format(d);
	}

	/**
	 * Parse a strict {@code yyyyMMdd} string into a {@link Date}.
	 * @throws Exception if the input is invalid (non-lenient).
	 */
	public static Date parseYyyyMMdd(String yyyymmdd) throws Exception {
		SimpleDateFormat fmt = new SimpleDateFormat("yyyyMMdd");
		fmt.setLenient(false);
		return fmt.parse(yyyymmdd);
	}

	/**
	 * Normalize various SPL time encodings to {@code yyyyMMdd} or return {@code null}
	 * if no reasonable normalization is possible.
	 * <ul>
	 *   <li>Trims non-digits (e.g., {@code 20211216-0500} or {@code 20211216000000}).</li>
	 *   <li>Accepts {@code yyyyMMdd}, {@code yyyyMM} (assumes day 01), or {@code yyyy} (assumes Jan 01).</li>
	 * </ul>
	 */
	public static String normalizeToYyyyMMdd(String raw) {
		if (raw == null) return null;
		String v = raw.trim().replaceAll("[^0-9]", "");
		if (v.length() >= 14) {          // yyyyMMddHHmmss...
			v = v.substring(0, 8);
		} else if (v.length() == 8) {    // yyyyMMdd (unchanged)
			// ok
		} else if (v.length() == 6) {    // yyyyMM -> assume day 01
			v = v + "01";
		} else if (v.length() == 4) {    // yyyy -> assume Jan 01
			v = v + "0101";
		} else {
			return null;
		}
		return v;
	}
}
