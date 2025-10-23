package org.pvlens.spl.processing.extract;

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

import org.pvlens.spl.om.Outcome;
import org.pvlens.spl.umls.Atom;

final class TextCleaner {
	private TextCleaner() {
	}

	static String removeExactTerms(String src, Outcome exact) {
		if (src == null || src.isBlank() || exact == null || exact.getCodes().isEmpty())
			return (src == null ? "" : src);
		String result = src;
		for (Atom a : exact.getCodes()) {
			String t = a.getTerm();
			if (t != null && !t.isBlank()) {
				result = result.replaceAll("\\b" + Pattern.quote(t) + "\\b", "");
			}
		}
		return result.trim();
	}
}