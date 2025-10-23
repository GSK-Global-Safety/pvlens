package org.pvlens.spl.processing;

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

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.pvlens.spl.om.SplDrug;
import org.pvlens.spl.om.Srlc;
import org.pvlens.spl.processing.extract.SplXmlLabelExtractor;
import org.pvlens.spl.processing.merge.ProductMerger;
import org.pvlens.spl.processing.merge.SrlcMerge;
import org.pvlens.spl.processing.persist.IdAllocators;
import org.pvlens.spl.processing.persist.ProductPersister;
import org.pvlens.spl.processing.persist.SqlWriters;
import org.pvlens.spl.processing.support.GlobalTrackers;
import org.pvlens.spl.umls.UmlsLoader;
import org.pvlens.spl.util.Logger;
import org.pvlens.spl.util.UmlsTerms;

public class SplProcessingPipeline {

	private final UmlsLoader umls;

	// Output writers
	HashMap<String, PrintWriter> OUTPUT_WRITERS = new HashMap<>();
	private int START_SUBSTANCE_ID = 100;

	// Data paths
	private static String OUTPUT_DIR;
	private static String OUTPUT_PRD_FILE;
	private static String OUTPUT_AE_FILE;
	private static String OUTPUT_IND_FILE;
	private static String OUTPUT_NDC_FILE;
	private static String OUTPUT_RXNORM_FILE;
	private static String OUTPUT_SNOMED_FILE;
	private static String OUTPUT_ATC_FILE;
	private static String OUTPUT_PRD_RELATED_FILE;

	private static String OUTPUT_AE_TEXT_FILE;
	private static String OUTPUT_IND_TEXT_FILE;
	private static String OUTPUT_BOX_TEXT_FILE;

	// Global
	GlobalTrackers trackers = new GlobalTrackers();
	SqlWriters sqlWriters;
	IdAllocators ids;
	UmlsTerms umlsTerms = new UmlsTerms();
	ProductMerger merger;

	/**
	 * Create SplProcessing Pipeline
	 * 
	 * @param umls
	 * @param priorGuidMap
	 * @param xmlPath
	 * @param sqlOutputPath
	 */
	public SplProcessingPipeline(UmlsLoader umls, Map<String, Integer> priorGuidMap, String sqlOutputPath) {
		this.umls = umls;

		OUTPUT_DIR = requireDirPath("OUTPUT_DIR", sqlOutputPath);
		// XML_ROOT = requireDirPath("SPL_XML_ROOT", xmlPath);

		OUTPUT_PRD_FILE = OUTPUT_DIR + "substance.sql";
		OUTPUT_RXNORM_FILE = OUTPUT_DIR + "substance_to_rxnorm.sql";
		OUTPUT_SNOMED_FILE = OUTPUT_DIR + "substance_to_snomed.sql";
		OUTPUT_ATC_FILE = OUTPUT_DIR + "substance_to_atc.sql";
		OUTPUT_NDC_FILE = OUTPUT_DIR + "substance_to_ndc.sql";
		OUTPUT_AE_FILE = OUTPUT_DIR + "listed_aes.sql";
		OUTPUT_IND_FILE = OUTPUT_DIR + "listed_indications.sql";
		OUTPUT_PRD_RELATED_FILE = OUTPUT_DIR + "product_related.sql";

		// Raw text
		OUTPUT_AE_TEXT_FILE = OUTPUT_DIR + "product_ae_text.sql";
		OUTPUT_IND_TEXT_FILE = OUTPUT_DIR + "product_ind_text.sql";
		OUTPUT_BOX_TEXT_FILE = OUTPUT_DIR + "product_box_text.sql";

		// Setup the output writers
		this.setupOutputWriters();

		// This code is not yet working
		// Determine the starting substance ID to use
//		if (priorGuidMap != null) {
//			if (priorGuidMap.size() > 0) {
//				for (int pid : priorGuidMap.values()) {
//					if (pid > START_SUBSTANCE_ID)
//						START_SUBSTANCE_ID = pid + 1;
//				}
//			}
//		}

		// debug
		// Logger.log("Starting Substance ID: " + START_SUBSTANCE_ID);

		sqlWriters = new SqlWriters(OUTPUT_WRITERS);
		ids = IdAllocators.getInstance();

		// Create the MedDRA table once at startup
		umlsTerms.createMeddraTable();

		// Product merger
		merger = new ProductMerger(this.umls);
		return;
	}

	/**
	 * Setup our output streams
	 * 
	 * @return
	 */
	private void setupOutputWriters() {

		if (OUTPUT_WRITERS != null && OUTPUT_WRITERS.size() == 0) {
			try {
				OUTPUT_WRITERS.put("PRODUCT", new PrintWriter(OUTPUT_PRD_FILE));
				OUTPUT_WRITERS.put("RXNORM", new PrintWriter(OUTPUT_RXNORM_FILE));
				OUTPUT_WRITERS.put("SNOMED", new PrintWriter(OUTPUT_SNOMED_FILE));
				OUTPUT_WRITERS.put("ATC", new PrintWriter(OUTPUT_ATC_FILE));
				OUTPUT_WRITERS.put("NDC", new PrintWriter(OUTPUT_NDC_FILE));
				OUTPUT_WRITERS.put("AE", new PrintWriter(OUTPUT_AE_FILE));
				OUTPUT_WRITERS.put("IND", new PrintWriter(OUTPUT_IND_FILE));
				OUTPUT_WRITERS.put("PROD_RELATED", new PrintWriter(OUTPUT_PRD_RELATED_FILE));
				OUTPUT_WRITERS.put("AE_TEXT", new PrintWriter(OUTPUT_AE_TEXT_FILE));
				OUTPUT_WRITERS.put("IND_TEXT", new PrintWriter(OUTPUT_IND_TEXT_FILE));
				OUTPUT_WRITERS.put("BOX_TEXT", new PrintWriter(OUTPUT_BOX_TEXT_FILE));

			} catch (Exception e) {
				Logger.warn("Error creating output streams: " + e.toString());
			}
		}
		return;
	}

	public void run(ConcurrentLinkedQueue<SplDrug> all, List<Srlc> srlcs) {

		// If running in multi-year mode, we need to know what prior GUIDs have been
		// seen
		HashMap<String, Boolean> priorGuids = new HashMap<>();
		for (SplDrug spl : all) {
			priorGuids.put(spl.getGuid(), true);
		}

		var extractor = new SplXmlLabelExtractor();
		Logger.log("Passing to the extractor: " + all.size());

		// Process
		all.parallelStream().forEach(spl -> extractor.processGuid(spl, trackers.xmlIndPass, trackers.xmlAePass,
				trackers.xmlBoxPass, false, sqlWriters));

		return;
	}

	public ConcurrentLinkedQueue<SplDrug> runMerge(ConcurrentLinkedQueue<SplDrug> all, Map<Integer, Date> approvalDates, Map<Integer, String> approvalSponsors,
			Map<String, Integer> priorGuidMap) {
		all = merger.mergeAll(all, approvalDates, approvalSponsors, umlsTerms, priorGuidMap);
		return all;
	}

	public void updateSrlcData(ConcurrentLinkedQueue<SplDrug> all, List<Srlc> srlcs) {
		SrlcMerge.updateLabelsFromSrlc(all, srlcs);
		return;
	}

	public ConcurrentLinkedQueue<SplDrug> persist(ConcurrentLinkedQueue<SplDrug> all, Map<Integer, Date> approvalDates,
			Map<String, Integer> priorGuidMap) {
		// Save the supporting tables
		merger.saveSupportTables(all, umlsTerms);

		ProductPersister persist = new ProductPersister(umls);
		persist.saveAll(all, sqlWriters, ids, trackers.splSrc, priorGuidMap);
		return all;
	}

	// In SplXmlLabelExtractor.java (top-level helpers)
	private static Path requirePath(String name, String val) {
		if (val == null || val.isBlank()) {
			throw new IllegalArgumentException("Missing required path config: " + name);
		}
		return Paths.get(val.trim());
	}

	private static String requireDirPath(String name, String val) {
		Path p = requirePath(name, val);
		// If you want to enforce existence:
		if (!Files.isDirectory(p))
			throw new IllegalArgumentException(name + " is not a directory: " + p);
		return val;
	}

	public void reviewFirstAddedDates(ConcurrentLinkedQueue<SplDrug> allProducts) {
		for (SplDrug drg : allProducts) {
			drg.reconcileLabelFirstAddedDatesWithinDrug();
		}
		return;
	}

}