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

import java.util.regex.Pattern;

/**
 * Helper class for mapping external drug dictionaries to PVLens
 * This class is not used in the PVLens processing pipeline
 */
public final class DoseText {
	private DoseText() {
	}

	// Units we’ll remove when paired with a number
	private static final String UNITS = "(?:mg|g|mcg|μg|µg|ug|ng|kg|iu|unit(?:s)?|u|meq|mmol|mol|ml|mL|l|L|%|ppm)";

	// 120 mg/5 mL, 10,000 units/mL, 0.1 mg per 5 mL, 4 mg/m^2, 2 mg/kg, 1 mg/hr
	private static final Pattern COMPOSITE = Pattern.compile("\\b\\d[\\d,]*(?:\\.\\d+)?\\s*-?\\s*" + UNITS
			+ "\\s*(?:/|per)\\s*\\d[\\d,]*(?:\\.\\d+)?\\s*-?\\s*(?:" + UNITS + "|m2|m\\^2|kg|hr|h|day|d)\\b",
			Pattern.CASE_INSENSITIVE);

	// 10–20 mg, 10-20 mg (range strength)
	private static final Pattern RANGE = Pattern.compile(
			"\\b\\d[\\d,]*(?:\\.\\d+)?\\s*(?:[-–]\\s*\\d[\\d,]*(?:\\.\\d+)?)\\s*" + UNITS + "\\b",
			Pattern.CASE_INSENSITIVE);

	// Simple strength: 100 mg, 100mg, 100-mg, 5%, 10,000 units
	private static final Pattern SIMPLE = Pattern.compile("\\b\\d[\\d,]*(?:\\.\\d+)?\\s*-?\\s*" + UNITS + "\\b",
			Pattern.CASE_INSENSITIVE);

	// Per-body or rate forms without a second number: 2 mg/kg, 1 mg/m^2, 5 mg/hr
	private static final Pattern PER_BODY_OR_RATE = Pattern.compile(
			"\\b\\d[\\d,]*(?:\\.\\d+)?\\s*-?\\s*" + UNITS + "\\s*(?:/|per)\\s*(?:kg|m2|m\\^2|hr|h|day|d)\\b",
			Pattern.CASE_INSENSITIVE);

	// Collapse any whitespace (incl. NBSP)
	private static final Pattern WS = Pattern.compile("[\\s\\u00A0]+");

	// Strip punctuation that can be left dangling after removals
	private static final Pattern PUNCT_GAPS = Pattern.compile("[\\(\\)\\[\\]\\{\\},;:/]+");

	/**
	 * Remove common dose/strength patterns from a drug string. 
	 */
	public static String removeUnits(String s) {
		if (s == null || s.isEmpty())
			return s;

		String out = s;

		// Normalize whitespace early (helps regexes)
		out = WS.matcher(out).replaceAll(" ").trim();

		// Remove composite/range/“per” forms first (most specific)
		out = COMPOSITE.matcher(out).replaceAll(" ");
		out = RANGE.matcher(out).replaceAll(" ");
		out = PER_BODY_OR_RATE.matcher(out).replaceAll(" ");

		// Then remove simple numeric + unit
		out = SIMPLE.matcher(out).replaceAll(" ");

		// Clean leftover punctuation/parentheses and repeated spaces
		out = PUNCT_GAPS.matcher(out).replaceAll(" ");
		out = out.replaceAll("\\(\\s*\\)", " "); // empty parentheses if any
		out = WS.matcher(out).replaceAll(" ").trim();

		return out;
	}

}
