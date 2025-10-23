package org.pvlens.spl.conf;

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


import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

public final class LabelTypeFilter {

	/** Default classpath resource. */
	public static final String DEFAULT_CLASSPATH_RESOURCE = "config/loinc-filter.properties";

	// --------------------------------------------------------------------------
	private final Properties properties = new Properties();

	public enum Mode {
		INCLUDE, EXCLUDE, OFF
	}

	private Mode mode;
	private Set<String> include;
	private Set<String> exclude;

	/**
	 * Create a loader that reads the default classpath resource:
	 * {@value #DEFAULT_CLASSPATH_RESOURCE}. If a system property
	 * {@value #SYS_PROP_CONFIG_PATH} is set, it takes precedence and the file at
	 * that path is used instead.
	 */
	public LabelTypeFilter() {
		loadFromClasspath(DEFAULT_CLASSPATH_RESOURCE);

		// Load from properties
		String m = properties.getProperty("mode", "off");
		mode = switch (m) {
		case "include" -> Mode.INCLUDE;
		case "exclude" -> Mode.EXCLUDE;
		default -> Mode.OFF;
		};

		// Update
		include = parseList(properties.getProperty("include", ""));
		exclude = parseList(properties.getProperty("exclude", ""));
	}

	// -------------------------- Internals --------------------------------------
	private void loadFromClasspath(String resource) {
		try (InputStream in = getClass().getClassLoader().getResourceAsStream(resource)) {
			if (in == null) {
				log("ERROR", "Unable to find resource on classpath: " + resource);
				return;
			}
			properties.load(in);
		} catch (Exception ex) {
			log("ERROR", "Failed to load properties from classpath: " + resource + " :: " + ex.getMessage());
		}
	}

	private static Set<String> parseList(String raw) {
		if (raw == null)
			return Set.of();
		// allow commas, whitespace, newlines
		return Arrays.stream(raw.split("[,\\s]+")).map(String::trim).filter(s -> !s.isEmpty())
				.collect(Collectors.toUnmodifiableSet());
	}

	/** Returns true if this LOINC should be processed. */
	public boolean allow(String loinc) {
		if (loinc == null || loinc.isBlank())
			return false;
		return switch (mode) {
		case INCLUDE -> include.contains(loinc);
		case EXCLUDE -> !exclude.contains(loinc);
		case OFF -> true;
		};
	}

	public Mode mode() {
		return mode;
	}

	public Set<String> include() {
		return include;
	}

	public Set<String> exclude() {
		return exclude;
	}

	private static void log(String level, String msg) {
		// Minimal logging shim; swap to your Logger if preferred:
		System.err.println("[" + level + "] " + msg);
	}
}
