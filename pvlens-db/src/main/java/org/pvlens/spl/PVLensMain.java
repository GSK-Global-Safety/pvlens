package org.pvlens.spl;

/*
 * This file is part of PVLens.
 *
 * Copyright (C) 2025 GlaxoSmithKline
 * Authored by: Jeffery Painter
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

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import org.pvlens.spl.conf.ConfigLoader;
import org.pvlens.spl.om.SplDrug;
import org.pvlens.spl.om.Srlc;
import org.pvlens.spl.processing.SplProcessingPipeline;
import org.pvlens.spl.umls.UmlsLoader;
import org.pvlens.spl.util.Logger;
import org.pvlens.spl.util.SrlcProcessor;
import org.pvlens.spl.util.ZipFileExtractor;

/**
 * Main entry point for PVLens SPL extraction.
 *
 * References: - https://academic.oup.com/nar/article/44/D1/D1075/2502602 -
 * https://github.com/deepchem/deepchem/blob/master/deepchem/molnet/load_function/sider_datasets.py
 * - https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3913703/ -
 * https://academic.oup.com/bioinformatics/article/37/15/2221/5988483 -
 * http://lmmd.ecust.edu.cn/metaadedb/basic_search.php
 *
 * SPL data:
 * https://dailymed.nlm.nih.gov/dailymed/spl-resources-all-drug-labels.cfm
 */
public class PVLensMain {

	// Year bounds for SPL directory validation
	private static final int MIN_YEAR = 2005;
	private static final int MAX_YEAR = ZonedDateTime.now(ZoneOffset.UTC).getYear() + 1;

	private final ConfigLoader cfg;

	public PVLensMain() {
		this.cfg = new ConfigLoader();
	}

	/**
	 * Application entry point.
	 */
	public static void main(String[] args) {
		PVLensMain app = new PVLensMain();
		app.run();
	}

	/**
	 * Orchestrates the SPL mapping pipeline: - Discover and extract SPL XMLs - Load
	 * SRLC and approval date metadata - Map GUIDs to products via UMLS - Run
	 * extraction, merge, reconciliation, and persistence
	 */
	private void run() {
		String splDataPath = cfg.getSplPath();
		String sqlOutputPath = cfg.getSqlOutputPath();

		// Prior GUID→SUBSTANCE_ID map (optional; placeholder for evolving workflow)
		// Note - this is not working and needs to be developed
		String priorSubstanceMapPath = cfg.getCsvOutputPath() + "/prior_guid_substance_map_file.csv";
		HashMap<String, Integer> priorGuidMap = loadPriorGuidMapFromFile(priorSubstanceMapPath);
		if (priorGuidMap.size() > 0) {
			Logger.log("Loaded prior substance map: " + priorGuidMap.size());
		}

		// Track GUID→XML mappings
		Map<String, List<String>> guidToXml = new HashMap<>();
		HashMap<String, String> guidSrcType = new HashMap<>();

		// Extract XML files from downloaded SPL archives
		Logger.log("Begin XML extraction...");

		// If SPL data is organized by year subdirectories, process per-year
		List<Integer> splYears = getSplYears(splDataPath);
		if (!splYears.isEmpty()) {
			Logger.log("Years of data found: " + splYears.size());
			Collections.sort(splYears);
			for (int year : splYears) {
				extractGuidsByYear(splDataPath, guidToXml, guidSrcType, year);
			}
		} else {
			// Flat layout (no year subdirectories)
			String splZipXmlMap = splDataPath + "SPL_ZIP_XML_MAP.csv";
			ZipFileExtractor zfe = new ZipFileExtractor(splDataPath);
			zfe.extractXmlFiles();
			ZipFileExtractor.getGuidXmlMap(splDataPath, guidToXml, guidSrcType, splZipXmlMap);
		}

		Logger.log("GUID count: " + guidToXml.size());

		// Load Safety-Related Label Change (SRLC) data
		List<Srlc> srlcEntries = SrlcProcessor.loadSrlcData();

		// Load FDA approval dates and sponsors
		HashMap<Integer, Date> approvalDates = loadApprovalDates();
		HashMap<Integer, String> approvalSponsors = loadApprovalSponsors();

		// Map GUIDs to products using UMLS
		UmlsLoader umls = UmlsLoader.getInstance();
		Logger.log("UMLS load completed");

		ConcurrentLinkedQueue<SplDrug> allProducts = umls.getMappedGuid(guidToXml);
		Logger.log("UMLS Mapped drugs: " + allProducts.size());

		// Pipeline processor
		SplProcessingPipeline splPipeline = new SplProcessingPipeline(umls, priorGuidMap, sqlOutputPath);

		splPipeline.run(allProducts, srlcEntries);
		Logger.log("All products prior to merge: " + allProducts.size());

		allProducts = splPipeline.runMerge(allProducts, approvalDates, approvalSponsors, priorGuidMap);
		Logger.log("All products post to merge: " + allProducts.size());

		Logger.log("Update label date information from SRLC data extract");
		splPipeline.updateSrlcData(allProducts, srlcEntries);

		Logger.log("Reconcile first add date for AEs and indications");
		splPipeline.reviewFirstAddedDates(allProducts);

		Logger.log("Products to save: " + allProducts.size());
		splPipeline.persist(allProducts, approvalDates, priorGuidMap);

		Logger.log("All products: " + allProducts.size());
		Logger.log("End");
	}

	/**
	 * Load prior GUID→SUBSTANCE_ID mappings from CSV (optional input). Provides
	 * basic conflict detection and summary statistics.
	 *
	 * @param infile Path to CSV file
	 * @return GUID→SUBSTANCE_ID map (empty if missing or unreadable)
	 */
	private HashMap<String, Integer> loadPriorGuidMapFromFile(String infile) {
		HashMap<String, Integer> guidMap = new HashMap<>();

		File infileFile = new File(infile);
		if (!infileFile.exists() || !infileFile.isFile()) {
			Logger.log("File does not exist: " + infile);
			return guidMap; // return empty map
		}

		// Track conflicts and stats
		Map<String, Set<Integer>> guidConflicts = new LinkedHashMap<>(); // GUID -> {ids...} when >1 id seen
		Map<Integer, Set<String>> idToGuids = new HashMap<>(); // SUBSTANCE_ID -> {guids...}
		int row = 0, inserted = 0, dupExact = 0, badRows = 0;

		// CSV format with delimiter, header, etc.
		CSVFormat csvFormat = CSVFormat.DEFAULT.withDelimiter(',').withHeader().withIgnoreHeaderCase().withTrim();

		try (FileReader reader = new FileReader(infile); CSVParser csvParser = new CSVParser(reader, csvFormat)) {
			for (CSVRecord record : csvParser) {
				row++;
				try {
					String guidRaw = record.get("GUID");
					String sidRaw = record.get("SUBSTANCE_ID");

					if (guidRaw == null || guidRaw.isEmpty() || sidRaw == null || sidRaw.isEmpty()) {
						badRows++;
						continue;
					}

					int substanceId = Integer.parseInt(sidRaw);
					if (substanceId <= 0) {
						badRows++;
						continue;
					}

					// Sanity check #1: GUID -> multiple IDs (conflict)
					Integer prev = guidMap.get(guidRaw);
					if (prev == null) {
						guidMap.put(guidRaw, substanceId);
						inserted++;
					} else if (!prev.equals(substanceId)) {
						// Record the conflict; keep the first mapping (do NOT overwrite)
						guidConflicts.computeIfAbsent(guidRaw, g -> new LinkedHashSet<>()).add(prev);
						guidConflicts.get(guidRaw).add(substanceId);
					} else {
						// exact duplicate row (same GUID, same ID)
						dupExact++;
					}

					// Sanity check #2: summarize ID -> multiple GUIDs
					idToGuids.computeIfAbsent(substanceId, k -> new LinkedHashSet<>()).add(guidRaw);

				} catch (Exception f) {
					badRows++;
					Logger.log("Parsing error on line " + row + ": " + f.getMessage());
				}
			}
		} catch (Exception e) {
			Logger.log("Error processing prior substance mapping: " + e.toString());
		}

		// Emit results and warnings
		Logger.log("Prior GUID map loaded: distinct GUIDs=" + guidMap.size() + ", inserted=" + inserted
				+ ", exact-duplicates=" + dupExact + ", bad-rows=" + badRows);

		if (!guidConflicts.isEmpty()) {
			Logger.log("WARNING: Conflicting GUID mappings detected (GUID -> multiple SUBSTANCE_IDs). Count="
					+ guidConflicts.size());
			int shown = 0, maxShow = 25;
			for (Map.Entry<String, Set<Integer>> e : guidConflicts.entrySet()) {
				Logger.log("  CONFLICT GUID=" + e.getKey() + " maps to IDs " + e.getValue());
				if (++shown >= maxShow) {
					Logger.log("  ... (" + (guidConflicts.size() - shown) + " more)");
					break;
				}
			}
		} else {
			Logger.log("No conflicting GUID->SUBSTANCE_ID mappings found.");
		}

		long idsWithMultipleGuids = idToGuids.values().stream().filter(s -> s.size() > 1).count();
		Logger.log("SUBSTANCE_IDs with >1 GUID: " + idsWithMultipleGuids
				+ " (informational; often expected after merges)");

		return guidMap;
	}

	/**
	 * Detect year subdirectories within the SPL data root.
	 *
	 * @param dataPath Root path containing SPL archives
	 * @return List of year directories found (empty if none or path invalid)
	 */
	private List<Integer> getSplYears(String dataPath) {
		List<Integer> years = new ArrayList<>();
		try {
			Path directory = Paths.get(dataPath);
			if (!Files.isDirectory(directory)) {
				Logger.error("SPL data directory does not exist: " + dataPath);
				return Collections.emptyList();
			}

			try (Stream<Path> stream = Files.list(directory)) {
				List<Path> directories = stream.filter(Files::isDirectory).collect(Collectors.toList());

				for (Path subdir : directories) {
					// Use directory name only (avoid string replace pitfalls)
					String name = subdir.getFileName().toString();
					try {
						int year = Integer.parseInt(name);
						if (year >= MIN_YEAR && year <= MAX_YEAR) {
							years.add(year);
						}
					} catch (Exception f) {
						// non-year subdirectory: ignore
					}
				}
			} catch (IOException e) {
				Logger.error("Error listing files: " + e.getMessage());
			}

		} catch (Exception e) {
			Logger.log("Error: " + e.toString());
		}

		return years;
	}

	/**
	 * Extract GUID maps for a given year directory.
	 *
	 * @param splDataPath Root SPL path
	 * @param guidToXml   Output map of GUID → XML file paths
	 * @param guidSrcType Output map of GUID → source type
	 * @param year        Year subdirectory to process
	 */
	private void extractGuidsByYear(String splDataPath, Map<String, List<String>> guidToXml,
			HashMap<String, String> guidSrcType, int year) {
		Logger.log("Begin XML extraction: " + year);
		String curYearPath = splDataPath + year + "/";
		Path dataPath = Paths.get(curYearPath);
		if (Files.exists(dataPath) && Files.isDirectory(dataPath)) {
			// SPL archives to process
			String splZipXmlMap = curYearPath + "SPL_ZIP_XML_MAP.csv";
			// If previously run, extractXmlFiles() is effectively idempotent
			ZipFileExtractor zfe = new ZipFileExtractor(curYearPath);
			zfe.extractXmlFiles();
			ZipFileExtractor.getGuidXmlMap(curYearPath, guidToXml, guidSrcType, splZipXmlMap);
			Logger.log("GUID count: " + guidToXml.size());
		}
	}

	/**
	 * Load FDA approval dates from Drugs@FDA Submissions.txt. Keeps the earliest
	 * approved ORIGINAL submission date per application.
	 *
	 * Expected headers in Submissions.txt (tab-delimited): ApplNo,
	 * SubmissionClassCodeID, SubmissionType, SubmissionNo, SubmissionStatus,
	 * SubmissionStatusDate, SubmissionsPublicNotes, ReviewPriority
	 *
	 * @return Map of application number → earliest approval date
	 */
	private HashMap<Integer, Date> loadApprovalDates() {
		HashMap<Integer, Date> approvalDates = new HashMap<>();
		String fdaSubmissionFile = cfg.getFdaApprovalFile(); // should point to Submissions.txt

		// Robust date parsers (try multiple formats commonly seen in dumps)
		final List<SimpleDateFormat> parsers = Arrays.asList(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"),
				new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss"), new SimpleDateFormat("yyyy-MM-dd"));
		// Ensure strict parsing
		parsers.forEach(p -> p.setLenient(false));

		// Tab-delimited with header
		CSVFormat tsv = CSVFormat.DEFAULT.withDelimiter('\t').withHeader().withIgnoreHeaderCase().withTrim();

		int lineNo = 0;
		try (FileReader reader = new FileReader(fdaSubmissionFile); CSVParser csv = new CSVParser(reader, tsv)) {

			for (CSVRecord r : csv) {
				lineNo++;
				try {
					// Required fields
					String subType = r.get("SubmissionType"); // e.g., ORIG, SUPPL, etc.
					String subStatus = r.get("SubmissionStatus"); // e.g., AP, PN, WD, etc.
					String dateStr = r.get("SubmissionStatusDate"); // e.g., 1969-07-16 00:00:00
					String applStr = r.get("ApplNo");

					if (subType == null || subStatus == null || dateStr == null || applStr == null) {
						continue; // skip incomplete rows
					}

					// Filter: original + approved
					if (!"ORIG".equalsIgnoreCase(subType.trim()))
						continue;
					if (!"AP".equalsIgnoreCase(subStatus.trim()))
						continue;

					int applNo = Integer.parseInt(applStr.trim());
					Date parsed = parseFirstMatch(dateStr.trim(), parsers);
					if (parsed == null)
						continue;

					// Keep earliest date per application
					Date existing = approvalDates.get(applNo);
					if (existing == null || parsed.before(existing)) {
						approvalDates.put(applNo, parsed);
					}

				} catch (Exception rowEx) {
					Logger.log("Parsing error on Submissions.txt line " + lineNo + ": " + rowEx.getMessage());
				}
			}

		} catch (Exception e) {
			Logger.log("Error processing Submissions.txt: " + e.toString());
		}

		return approvalDates;
	}

	/**
	 * Link NDA to label sponsors/manufacturers
	 *
	 * @return Map of application number → registered sponsor
	 */
	private HashMap<Integer, String> loadApprovalSponsors() {
		HashMap<Integer, String> applicationSponsors = new HashMap<>();
		String fdaSubmissionFile = cfg.getFdaApplicationsFile();

		// Tab-delimited with header
		CSVFormat tsv = CSVFormat.DEFAULT.withDelimiter('\t').withHeader().withIgnoreHeaderCase().withTrim();

		int lineNo = 0;
		try (FileReader reader = new FileReader(fdaSubmissionFile); CSVParser csv = new CSVParser(reader, tsv)) {

			for (CSVRecord r : csv) {
				lineNo++;
				try {
					// Required fields
					String applStr = r.get("ApplNo");
					String appSponsor = r.get("SponsorName");

					if (appSponsor == null || applStr == null) {
						continue; // skip incomplete rows
					}
					int applNo = Integer.parseInt(applStr.trim());
					applicationSponsors.put(applNo, appSponsor);

				} catch (Exception rowEx) {
					Logger.log("Parsing error on Applications.txt line " + lineNo + ": " + rowEx.getMessage());
				}
			}

		} catch (Exception e) {
			Logger.log("Error processing Submissions.txt: " + e.toString());
		}

		return applicationSponsors;
	}

	/** Try multiple date formats, return first that parses, else null. */
	private Date parseFirstMatch(String s, List<SimpleDateFormat> formats) {
		for (SimpleDateFormat f : formats) {
			try {
				// Normalize common oddities
				String x = s.replace('T', ' '); // allow ISO-like strings
				// Some dumps include trailing ".0" or timezone-less seconds missing
				if (x.matches("^\\d{4}-\\d{2}-\\d{2}$")) {
					return formats.get(formats.size() - 1).parse(x); // yyyy-MM-dd
				}
				return f.parse(x);
			} catch (Exception ignore) {
			}
		}
		return null;
	}

}
