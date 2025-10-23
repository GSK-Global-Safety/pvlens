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

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * Loads configuration for the SPL extractor from a {@code .properties} file.
 * <p>
 * By default, this loader reads <code>config/spldb.properties</code> from the
 * classpath. You can override this by setting the system property
 * <code>pvlens.config</code> to an absolute path, or by using the
 * {@link #ConfigLoader(Path)} constructor.
 *
 * <h3>Notes</h3>
 * <ul>
 * <li>All directory-like values are normalized to end with a trailing slash
 * (e.g. <code>/path/to/dir/</code>).</li>
 * <li>{@link #getParallelProcessingProductLimit()} supports the new key
 * <code>PARALLEL_PRODUCT_LIMIT</code> and (deprecated)
 * <code>PRODUCT_UPDATE_LIMIT</code>.</li>
 * <li>Use {@link #validate()} during startup to check for missing required
 * keys.</li>
 * </ul>
 */
public final class ConfigLoader {

	/** Default classpath resource. */
	public static final String DEFAULT_CLASSPATH_RESOURCE = "config/spldb.properties";

	private LabelTypeFilter otherLoincFilter;

	// ---- Property keys (centralized, easy to search/maintain) ----------------
	private static final String K_SRLC_PATH = "SRLC_PATH";
	private static final String K_SRLC_LABEL_CHANGES = "SRLC_LABEL_CHANGES";

	// FDA NDA dates
	private static final String K_FDA_APPROVAL_FILE = "FDA_APPROVAL_FILE";
	private static final String K_FDA_APPLICATION_FILE = "FDA_APPLICATION_FILE";
	
	
	// FDA BioSimilars
	private static final String K_BIOSIMILAR_FILE = "BIOSIMILAR_FILE";
	private static final String K_BIOSIMILAR_ENRICHED_FILE = "BIOSIMILAR_ENRICHED_FILE";

	private static final String K_SPL_PATH = "SPL_PATH";
	private static final String K_SPL_PRODUCT_LABEL_MAP = "SPL_PRODUCT_LABEL_MAP";
	private static final String K_SPL_ZIP_XML_MAP = "SPL_ZIP_XML_MAP";


	private static final String K_SQL_OUTPUT_PATH = "SQL_OUTPUT_PATH";
	private static final String K_CSV_OUTPUT_PATH = "CSV_OUTPUT_PATH";

	private static final String K_DB_DRIVER = "DB_DRIVER";
	private static final String K_DB_HOST = "DB_HOST";
	private static final String K_DB_PORT = "DB_PORT";
	private static final String K_DB_USER = "DB_USER";
	private static final String K_DB_PASS = "DB_PASS";
	private static final String K_DB_NAME = "DB_NAME";
	private static final String K_UMLS_DB_NAME = "UMLS_DB_NAME";

	// Parallelism keys (new + deprecated)
	private static final String K_PARALLEL_PRODUCT_LIMIT = "PARALLEL_PRODUCT_LIMIT";

	/** System property to point to an external config file. */
	public static final String SYS_PROP_CONFIG_PATH = "pvlens.config";

	// --------------------------------------------------------------------------

	private final Properties properties = new Properties();

	/**
	 * Create a loader that reads the default classpath resource:
	 * {@value #DEFAULT_CLASSPATH_RESOURCE}. If a system property
	 * {@value #SYS_PROP_CONFIG_PATH} is set, it takes precedence and the file at
	 * that path is used instead.
	 */
	public ConfigLoader() {
		// 1) explicit file via system property?
		String external = System.getProperty(SYS_PROP_CONFIG_PATH);
		if (external != null && !external.isBlank()) {
			Path p = Path.of(external.trim());
			if (Files.isReadable(p)) {
				loadFromFile(p);
				return;
			} else {
				log("WARN", "System property " + SYS_PROP_CONFIG_PATH + " points to an unreadable path: " + p);
			}
		}
		// 2) fallback to classpath resource
		loadFromClasspath(DEFAULT_CLASSPATH_RESOURCE);

		// LOINC filters for other product labels
		otherLoincFilter = new LabelTypeFilter();
	}

	public LabelTypeFilter getOtherLoincFilter() {
		return otherLoincFilter;
	}

	/**
	 * Create a loader that reads a specific file on disk.
	 *
	 * @param filePath absolute or relative path to a .properties file
	 * @throws IllegalArgumentException if the file is not readable
	 */
	public ConfigLoader(Path filePath) {
		if (filePath == null || !Files.isReadable(filePath)) {
			throw new IllegalArgumentException("Config file is null or not readable: " + filePath);
		}
		loadFromFile(filePath);
	}

	// -------------------------- Public API -------------------------------------

	/**
	 * Validates presence of keys that the application typically requires to run.
	 * This does not fail; it returns a list of human-readable issues so the caller
	 * can decide how to proceed (e.g., fail fast, show help, etc.).
	 *
	 * @return list of error strings; empty if all required keys look OK
	 */
	public List<String> validate() {
		List<String> issues = new ArrayList<>();

		// Required directories/files for core flows
		requireNonBlank(K_SPL_PATH, issues);
		requireNonBlank(K_SQL_OUTPUT_PATH, issues);
		requireNonBlank(K_CSV_OUTPUT_PATH, issues); // <-- NEW

		// Enforce SQL and CSV outputs are not the same directory
		String sqlOut = properties.getProperty(K_SQL_OUTPUT_PATH);
		String csvOut = properties.getProperty(K_CSV_OUTPUT_PATH);
		if (sqlOut != null && csvOut != null) {
			String nSql = normalizedDir(sqlOut);
			String nCsv = normalizedDir(csvOut);
			if (!nSql.isBlank() && nSql.equals(nCsv)) {
				issues.add("CSV_OUTPUT_PATH must differ from SQL_OUTPUT_PATH.");
			}
		}

		// DB essentials
		requireNonBlank(K_DB_DRIVER, issues);
		requireNonBlank(K_DB_HOST, issues);
		requireNonBlank(K_DB_PORT, issues);
		requireNonBlank(K_DB_USER, issues);
		requireNonBlank(K_DB_PASS, issues);
		requireNonBlank(K_DB_NAME, issues);

		// FDA BioSimilars
		requireNonBlank(K_BIOSIMILAR_FILE, issues);
		requireNonBlank(K_BIOSIMILAR_ENRICHED_FILE, issues);

		// FDA SRLC data
		requireNonBlank(K_SRLC_PATH, issues);
		requireNonBlank(K_SRLC_LABEL_CHANGES, issues);

		// Typos / common problems
		if (properties.containsKey("PRODUCT_LABEL_MAP")) {
			issues.add("Found deprecated/typo key 'PRODUCT_LABEL_MAP'. Use '" + K_SPL_PRODUCT_LABEL_MAP + "'.");
		}
		return issues;
	}

	/**
	 * Product update limit for parallel processing. Defaults to ~25% of available
	 * cores if not set. New key is {@code PARALLEL_PRODUCT_LIMIT}; the old
	 * {@code PRODUCT_UPDATE_LIMIT} is still honored (deprecated).
	 */
	public int getParallelProcessingProductLimit() {
		int cores = Math.max(1, Runtime.getRuntime().availableProcessors());
		int defaultLimit = Math.max(1, (int) Math.floor(cores / 4.0));

		String raw = getOptional(K_PARALLEL_PRODUCT_LIMIT, null);
		if (raw != null) {
			try {
				int val = Integer.parseInt(raw.trim());
				if (val <= 0)
					return defaultLimit;
				// sanity cap: never exceed core count
				return Math.min(val, cores);
			} catch (NumberFormatException nfe) {
				log("WARN", "Invalid integer for parallel limit: '" + raw + "'. Using default " + defaultLimit);
			}
		}
		return defaultLimit;
	}

	/** Path to the SPL root from the FDA archive. */
	public String getSplPath() {
		return normalizedDir(getRequired(K_SPL_PATH));
	}

	/**
	 * CSV file name that maps product names to SPL files (resides under
	 * SQL_OUTPUT_PATH).
	 */
	public String getSplProductLabelMap() {
		return getRequired(K_SPL_PRODUCT_LABEL_MAP);
	}

	/**
	 * CSV file name mapping ZIPs to extracted XML files (resides under
	 * SQL_OUTPUT_PATH).
	 */
	public String getZipToXmlMapFilename() {
		return getRequired(K_SPL_ZIP_XML_MAP);
	}

	/** Get the FDA approval dates file */
	public String getFdaApprovalFile() {
		return getRequired(K_FDA_APPROVAL_FILE);
	}
	
	public String getFdaApplicationsFile() {
		return getRequired(K_FDA_APPLICATION_FILE);
	}
	
	/** Directory for generated SQL files. */
	public String getSqlOutputPath() {
		return normalizedDir(getRequired(K_SQL_OUTPUT_PATH));
	}

	/** Directory for generated CSV files. */
	public String getCsvOutputPath() {
		return normalizedDir(getRequired(K_CSV_OUTPUT_PATH));
	}
	
	/** Hostname for the target relational database. */
	public String getDbHost() {
		return getRequired(K_DB_HOST);
	}

	/** Port for the target relational database. */
	public String getDbPort() {
		return getRequired(K_DB_PORT);
	}

	/** Database username. */
	public String getDbUser() {
		return getRequired(K_DB_USER);
	}

	/** Database password. */
	public String getDbPass() {
		return getRequired(K_DB_PASS);
	}

	/** Database/schema name. */
	public String getDbName() {
		return getRequired(K_DB_NAME);
	}

	/** JDBC driver class name. */
	public String getDbDriver() {
		return getRequired(K_DB_DRIVER);
	}

	/** Optional: local UMLS database/schema name if present. */
	public String getUmlsDbName() {
		return getOptional(K_UMLS_DB_NAME, "");
	}

	/** Directory containing SRLC data files generated by preprocessing. */
	public String getSrlcPath() {
		return normalizedDir(getRequired(K_SRLC_PATH));
	}

	/**
	 * SRLC label changes CSV file name (typically under {@link #getSrlcPath()}).
	 */
	public String getSrlcLabelChangeFile() {
		return getRequired(K_SRLC_LABEL_CHANGES);
	}

	public String getBiosimilarFile() {
		return getRequired(K_BIOSIMILAR_FILE);
	}

	public String getBiosimilarEnrichedFile() {
		return getRequired(K_BIOSIMILAR_ENRICHED_FILE);
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

	private void loadFromFile(Path file) {
		try (InputStream in = new FileInputStream(file.toFile())) {
			properties.load(in);
		} catch (Exception ex) {
			log("ERROR", "Failed to load properties from file: " + file + " :: " + ex.getMessage());
		}
	}

	private String getRequired(String key) {
		String v = getOptional(key, null);
		if (v == null || v.isBlank()) {
			throw new IllegalStateException("Missing required property: " + key);
		}
		return v;
	}

	private String getOptional(String key, String defaultVal) {
		String v = properties.getProperty(key);
		if (v == null)
			return defaultVal;
		v = v.trim();
		return v.isEmpty() ? defaultVal : v;
	}

	private String normalizedDir(String path) {
		if (path == null || path.isBlank())
			return path;
		String p = path.trim();
		if (!p.endsWith("/"))
			p = p + "/";
		return p;
	}

	private void requireNonBlank(String key, List<String> issues) {
		String v = properties.getProperty(key);
		if (v == null || v.trim().isEmpty()) {
			issues.add("Missing required property: " + key);
		}
	}

	private void warnIfBlank(String key, List<String> issues) {
		String v = properties.getProperty(key);
		if (v == null || v.trim().isEmpty()) {
			issues.add("Warning: optional property is blank: " + key);
		}
	}

	private static void log(String level, String msg) {
		// Minimal logging shim; swap to your Logger if preferred:
		System.err.println("[" + level + "] " + msg);
	}
}