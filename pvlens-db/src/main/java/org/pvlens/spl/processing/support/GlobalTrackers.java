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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Global, thread-safe trackers used across the pipeline for de-duplication and
 * simple counters. Designed for single-JVM batch runs.
 */
public final class GlobalTrackers {

	public final ConcurrentMap<String, Integer> ingredient = new ConcurrentHashMap<>();
	public final ConcurrentMap<String, Integer> rxnorm     = new ConcurrentHashMap<>();
	public final ConcurrentMap<String, Integer> ndc        = new ConcurrentHashMap<>();
	public final ConcurrentMap<String, Integer> splSrc     = new ConcurrentHashMap<>();
	public final ConcurrentMap<String, Integer> srlcId     = new ConcurrentHashMap<>();
	public final ConcurrentMap<String, Integer> written    = new ConcurrentHashMap<>();

	// XML section pass counters
	public final ConcurrentMap<String, Integer> xmlIndPass = new ConcurrentHashMap<>();
	public final ConcurrentMap<String, Integer> xmlAePass  = new ConcurrentHashMap<>();
	public final ConcurrentMap<String, Integer> xmlBoxPass = new ConcurrentHashMap<>();

	/**
	 * Pairwise registry mapping {@code "<code>\u0001<name>"} → {@code NDC_ID}.
	 * Useful for keeping a stable ID for a (code,name) combination across inserts.
	 */
	public static final ConcurrentMap<String, Integer> NDC_CODE_NAME_TO_ID = new ConcurrentHashMap<>();

	/** Separator used to build (code,name) composite keys in {@link #NDC_CODE_NAME_TO_ID}. */
	public static final char NDC_PAIR_SEP = '\u0001';

	public GlobalTrackers() { /* default */ }
}
