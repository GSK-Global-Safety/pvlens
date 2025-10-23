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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ConfigLoaderTest {

	@TempDir
	Path tmp;

	private String priorSysProp;

	@AfterEach
	void cleanupSysProp() {
		if (priorSysProp == null) {
			System.clearProperty(ConfigLoader.SYS_PROP_CONFIG_PATH);
		} else {
			System.setProperty(ConfigLoader.SYS_PROP_CONFIG_PATH, priorSysProp);
		}
	}

	// --- helpers -------------------------------------------------------------

	private Path writePropsFile(Properties p, String filename) throws IOException {
		Path f = tmp.resolve(filename);
		try (var out = Files.newOutputStream(f)) {
			p.store(out, "test");
		}
		return f;
	}

	private Properties minimalRequiredProps() {
		Properties p = new Properties();
		// Required directories/files
		p.setProperty("SPL_PATH", "/spl");
		p.setProperty("SPL_XML_PATH", "/spl/xml");
		p.setProperty("SQL_OUTPUT_PATH", "/out/sql");
		p.setProperty("CSV_OUTPUT_PATH", "/out/csv");

		p.setProperty("SRLC_PATH", "/fda_srlc");
		p.setProperty("SRLC_LABEL_CHANGES", "label_changes_test.csv");

		p.setProperty("SPL_PRODUCT_LABEL_MAP", "product_map.csv");
		p.setProperty("SPL_ZIP_XML_MAP", "zip_xml_map.csv");

		p.setProperty("FDA_APPROVAL_FILE", "src/main/resources/fda/approval_dates.csv");
		p.setProperty("BIOSIMILAR_FILE", "src/main/resources/fda/biosimilar_fda.xlsx");
		p.setProperty("BIOSIMILAR_ENRICHED_FILE", "src/main/resources/fda/biosimilar_fda_enriched.xlsx");

		// DB essentials
		p.setProperty("DB_DRIVER", "com.mysql.cj.jdbc.Driver");
		p.setProperty("DB_HOST", "localhost");
		p.setProperty("DB_PORT", "3306");
		p.setProperty("DB_USER", "user");
		p.setProperty("DB_PASS", "password");
		p.setProperty("DB_NAME", "dbname");

		return p;
	}

	// --- tests ---------------------------------------------------------------

	@Test
	void loads_from_file_and_normalizes_dirs_and_defaults() throws Exception {
		Properties p = minimalRequiredProps();
		// Explicitly leave CSV/REPORT paths empty to test defaulting to SQL_OUTPUT_PATH
		// Also leave DB_PASS empty => default "".
		p.setProperty("DB_PASS", "   "); // blank
		Path f = writePropsFile(p, "conf1.properties");

		ConfigLoader loader = new ConfigLoader(f);

		// Required path getters normalize with trailing slash
		assertEquals("/spl/", loader.getSplPath());
		assertEquals("/out/sql/", loader.getSqlOutputPath());
		assertEquals("/out/csv/", loader.getCsvOutputPath());

		// Specific simple getters
		assertEquals("product_map.csv", loader.getSplProductLabelMap());
		assertEquals("zip_xml_map.csv", loader.getZipToXmlMapFilename());

	}

	@Test
	void validate_reports_missing_required_and_warnings() throws Exception {
		// Deliberately sparse config -> most required keys absent/blank
		Properties p = new Properties();
		Path f = writePropsFile(p, "conf_missing.properties");

		ConfigLoader loader = new ConfigLoader(f);
		List<String> issues = loader.validate();

		// Required keys must be reported
		assertTrue(issues.stream().anyMatch(s -> s.contains("Missing required property: SPL_PATH")));
		assertTrue(issues.stream().anyMatch(s -> s.contains("Missing required property: SRLC_PATH")));
		assertTrue(issues.stream().anyMatch(s -> s.contains("Missing required property: SQL_OUTPUT_PATH")));
		assertTrue(issues.stream().anyMatch(s -> s.contains("Missing required property: CSV_OUTPUT_PATH")));
		assertTrue(issues.stream().anyMatch(s -> s.contains("Missing required property: SRLC_LABEL_CHANGES")));
		assertTrue(issues.stream().anyMatch(s -> s.contains("Missing required property: DB_DRIVER")));
		assertTrue(issues.stream().anyMatch(s -> s.contains("Missing required property: DB_HOST")));
		assertTrue(issues.stream().anyMatch(s -> s.contains("Missing required property: DB_PORT")));
		assertTrue(issues.stream().anyMatch(s -> s.contains("Missing required property: DB_USER")));
		assertTrue(issues.stream().anyMatch(s -> s.contains("Missing required property: DB_PASS")));
		assertTrue(issues.stream().anyMatch(s -> s.contains("Missing required property: DB_NAME")));
	}

	@Test
	void system_property_override_loads_external_file() throws Exception {
		Properties p = minimalRequiredProps();
		p.setProperty("CSV_OUTPUT_PATH", "/csv/path"); // set to see it reflected
		Path f = writePropsFile(p, "override.properties");

		// Save & set system property
		priorSysProp = System.getProperty(ConfigLoader.SYS_PROP_CONFIG_PATH);
		System.setProperty(ConfigLoader.SYS_PROP_CONFIG_PATH, f.toAbsolutePath().toString());

		ConfigLoader loader = new ConfigLoader(); // should pick up system property

		assertEquals("/csv/path/", loader.getCsvOutputPath()); // normalized
		assertEquals("zip_xml_map.csv", loader.getZipToXmlMapFilename());
	}

	@Test
	void parallel_limit_uses_new_key_and_caps_at_cores() throws Exception {
		Properties p = minimalRequiredProps();
		// Ask for something huge; should be capped at core count
		p.setProperty("PARALLEL_PRODUCT_LIMIT", "9999");
		Path f = writePropsFile(p, "parallel_new.properties");

		ConfigLoader loader = new ConfigLoader(f);
		int limit = loader.getParallelProcessingProductLimit();

		int cores = Math.max(1, Runtime.getRuntime().availableProcessors());
		assertEquals(cores, limit);
	}

	@Test
	void required_getters_throw_when_missing() throws Exception {
		Properties p = new Properties(); // empty
		Path f = writePropsFile(p, "missing_required.properties");
		ConfigLoader loader = new ConfigLoader(f);

		// Each required getter should throw an IllegalStateException when missing
		assertThrows(IllegalStateException.class, loader::getSplPath);
		assertThrows(IllegalStateException.class, loader::getSrlcPath);
		assertThrows(IllegalStateException.class, loader::getSrlcLabelChangeFile);
		assertThrows(IllegalStateException.class, loader::getFdaApprovalFile);
		assertThrows(IllegalStateException.class, loader::getSqlOutputPath);
		assertThrows(IllegalStateException.class, loader::getCsvOutputPath);
		assertThrows(IllegalStateException.class, loader::getDbHost);
		assertThrows(IllegalStateException.class, loader::getDbPort);
		assertThrows(IllegalStateException.class, loader::getDbUser);
		assertThrows(IllegalStateException.class, loader::getDbPass);
		assertThrows(IllegalStateException.class, loader::getDbName);
		assertThrows(IllegalStateException.class, loader::getDbDriver);
	}

	@Test
	void validate_flags_when_csv_and_sql_paths_are_the_same() throws Exception {
		Properties p = minimalRequiredProps();
		p.setProperty("SQL_OUTPUT_PATH", "/out/same");
		p.setProperty("CSV_OUTPUT_PATH", "/out/same"); // same on purpose

		Path f = writePropsFile(p, "same_dirs.properties");
		ConfigLoader loader = new ConfigLoader(f);

		List<String> issues = loader.validate();
		assertTrue(issues.stream().anyMatch(s -> s.contains("CSV_OUTPUT_PATH must differ")));
	}

	@Test
	void getters_return_normalized_distinct_paths() throws Exception {
		Properties p = minimalRequiredProps();
		p.setProperty("SQL_OUTPUT_PATH", "/out/sql");
		p.setProperty("CSV_OUTPUT_PATH", "/out/csv");
		// Optional report missing -> should default to SQL dir

		Path f = writePropsFile(p, "distinct_dirs.properties");
		ConfigLoader loader = new ConfigLoader(f);

		assertEquals("/out/sql/", loader.getSqlOutputPath());
		assertEquals("/out/csv/", loader.getCsvOutputPath());
		assertTrue(loader.validate().isEmpty());
	}

}
