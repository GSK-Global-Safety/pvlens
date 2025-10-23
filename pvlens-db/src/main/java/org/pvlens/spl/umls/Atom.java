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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import lombok.Data;

/**
 * Represents a UMLS atom: an atomic concept identified by AUI/CUI with a
 * preferred term/code and term type (TTY). This model is used for matching and
 * for generating export rows compatible with UMLS/MedDRA subsets.
 * 
 * Author: Jeffery Painter Created: 2024-08-23 Updated: 2025-08-25
 */
@Data
public class Atom {

	// --- Constants ------------------------------------------------------------

	/** Default delimiter used by MRCONSO-like exports. */
	private static final String MRCONSO_DELIM = "|";

	/** MedDRA source abbreviation used in MRCONSO-like exports. */
	private static final String SAB_MDR = "MDR";

	// --- Core identity --------------------------------------------------------

	/** Internal database id (optional; -1 when unset). */
	private int databaseId = -1;

	/** Atom Unique Identifier (UMLS). */
	private String aui;

	/** Concept Unique Identifier (UMLS). */
	private String cui;

	/** Human-readable term string for the atom. */
	private String term;

	/** Term Type (TTY), e.g., PT, LLT, etc. */
	private String tty;

	/** Preferred Term code (e.g., MedDRA PT code). */
	private String ptCode;

	/** Source-specific code for this atom. */
	private String code;

	/** Preferred flag (Y/N or 1/0 depending on source). */
	private String isPref;

	/** Source abbreviation (e.g., MDR). */
	private String sab;

	// --- Hierarchy / Topology -------------------------------------------------

	/** Path to root (if present) as source-provided identifiers. */
	private String[] ptr;

	/** Parent atom ids (local db ids or other agreed key). */
	private List<Long> parents = new ArrayList<>();

	/** Child atom ids (local db ids or other agreed key). */
	private List<Long> children = new ArrayList<>();

	// --- Constructors ---------------------------------------------------------

	public Atom() {
	}

	public Atom(String aui, String cui, String ptCode, String code, String term, String tty) {
		this.aui = aui;
		this.cui = cui;
		this.ptCode = ptCode;
		this.code = code;
		this.term = term;
		this.tty = tty;
	}

	// Defensive setters for collections/arrays (avoid null surprises)
	public void setParents(List<Long> parents) { // note
		this.parents = (parents == null) ? new ArrayList<>() : new ArrayList<>(parents);
	}

	public void setChildren(List<Long> children) { // note
		this.children = (children == null) ? new ArrayList<>() : new ArrayList<>(children);
	}

	public void setPtr(String[] ptr) { // note
		this.ptr = (ptr == null) ? null : Arrays.copyOf(ptr, ptr.length);
	}

	// --- Matching -------------------------------------------------------------

	/**
	 * Returns true iff: • CUI matches exactly • TTY matches exactly • term matches
	 * case-insensitively and trimmed
	 */
	public boolean isMatch(String cui, String tty, String term) {
		if (this.getTty() == null || tty == null || !this.getTty().equals(tty))
			return false;
		if (this.getCui() == null || cui == null || !this.getCui().equals(cui))
			return false;

		final String normalizedTerm = normalizeTerm(term);
		final String normalizedAtomTerm = normalizeTerm(this.getTerm());
		return normalizedAtomTerm.equals(normalizedTerm);
	}

	private static String normalizeTerm(String s) {
		return (s == null) ? "" : s.trim().toLowerCase(Locale.ROOT);
	}

	// --- Exports --------------------------------------------------------------

	/**
	 * MRCONSO-like pipe-delimited row; maintains trailing delimiter for positional
	 * safety.
	 *
	 * Layout (indexes for reference): 0 CUI | 1 | 2 | 3 | 4 | 5 | 6 | 7 AUI | 8 | 9
	 * | 10 PT_CODE | 11 SAB | 12 TTY | 13 CODE | 14 TERM |
	 */
	public String getOutputRecord() {
		String d = MRCONSO_DELIM;
		StringBuilder sb = new StringBuilder(256);
		sb.append(nz(this.getCui())).append(d) // 0
				.append(d) // 1
				.append(d) // 2
				.append(d) // 3
				.append(d) // 4
				.append(d) // 5
				.append(d) // 6
				.append(nz(this.getAui())).append(d) // 7
				.append(d) // 8
				.append(d) // 9
				.append(nz(this.getPtCode())).append(d) // 10
				.append(SAB_MDR).append(d) // 11
				.append(nz(this.getTty())).append(d) // 12
				.append(nz(this.getCode())).append(d) // 13
				.append(nz(this.getTerm())).append(d); // 14 (trailing delim)
		return sb.toString();
	}

	/** MedDRA-centric CSV: ID | AUI | CUI | CODE | PT_CODE | TERM | TTY */
	public String toMdrCsvString(String delimiter) {
		String d = validDelim(delimiter);
		StringBuilder sb = new StringBuilder(128);
		sb.append(this.getDatabaseId()).append(d).append(nz(this.getAui())).append(d).append(nz(this.getCui()))
				.append(d).append(nz(this.getCode())).append(d).append(nz(this.getPtCode())).append(d)
				.append(nz(this.getTerm())).append(d).append(nz(this.getTty()));
		return sb.toString();
	}

	/** Generic CSV: ID | AUI | CUI | CODE | TERM | TTY (no PT_CODE) */
	public String toCsvString(String delimiter) {
		String d = validDelim(delimiter);
		StringBuilder sb = new StringBuilder(128);
		sb.append(this.getDatabaseId()).append(d).append(nz(this.getAui())).append(d).append(nz(this.getCui()))
				.append(d).append(nz(this.getCode())).append(d).append(nz(this.getTerm())).append(d)
				.append(nz(this.getTty()));
		return sb.toString();
	}

	// --- Small utilities ------------------------------------------------------
	private static String nz(String s) {
		return (s == null) ? "" : s;
	}

	private static String validDelim(String d) {
		return (d == null || d.isEmpty()) ? MRCONSO_DELIM : d;
	}
}
