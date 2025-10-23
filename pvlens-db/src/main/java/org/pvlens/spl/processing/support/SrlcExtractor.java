package org.pvlens.spl.processing.support;

import java.io.FileInputStream;

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

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.xml.XMLConstants;
import javax.xml.namespace.NamespaceContext;

import org.pvlens.spl.conf.ConfigLoader;
import org.pvlens.spl.om.Outcome;
import org.pvlens.spl.processing.extract.SplXmlLabelExtractor;
import org.pvlens.spl.umls.Atom;
import org.pvlens.spl.umls.UmlsLoader;
import org.pvlens.spl.util.Logger;
import org.pvlens.spl.util.MedDRAProcessor;

import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.tokenize.TokenizerModel;

public class SrlcExtractor {

	// Keep global trackers visible throughout the class
	HashMap<String, ConcurrentHashMap<String, Integer>> globalTrackers;

	private static UmlsLoader umls;

	// Load your sentence model once (whatever you already use to load it)
	private static String SENT_MODEL_PATH = "models/en-sent.bin";
	private static final opennlp.tools.sentdetect.SentenceModel SENTENCE_MODEL = loadSentModel();

	// Thread-local detector (each thread gets its own instance)
	private static final ThreadLocal<opennlp.tools.sentdetect.SentenceDetectorME> SENT_DETECTOR = ThreadLocal
			.withInitial(() -> new opennlp.tools.sentdetect.SentenceDetectorME(SENTENCE_MODEL));

	// Precompiled, thread-safe regex patterns
	private static final java.util.regex.Pattern WS_NBSP = java.util.regex.Pattern.compile("[\\s\\u00A0]+");
	// Accepts "(5%)" or "(12.3%)"
	private static final java.util.regex.Pattern PCT_PAREN = java.util.regex.Pattern.compile("\\(\\d+(?:\\.\\d+)?%\\)");

	public SrlcExtractor() {
		SrlcExtractor.umls = UmlsLoader.getInstance();
		return;
	}

	
	private static SentenceModel loadSentModel() {
		
		try (InputStream in = tryOpen(SENT_MODEL_PATH)) {
			if (in == null)
				return null;
			return new SentenceModel(in);
		} catch (Exception e) {
			return null;
		}		
	}

	// === Lazy model loading helpers ===
	private static InputStream tryOpen(String path) {
		// FS first
		try {
			java.nio.file.Path p = java.nio.file.Paths.get(path);
			if (java.nio.file.Files.exists(p))
				return new FileInputStream(p.toFile());
		} catch (Exception ignore) {
		}
		// CLASSPATH
		try {
			InputStream in = SrlcExtractor.class.getClassLoader().getResourceAsStream(path);
			if (in != null)
				return in;
		} catch (Exception ignore) {
		}
		return null;
	}

	
	/**
	 * Process single extracted text component
	 * 
	 * @param zipFile
	 * @param aeType
	 * @param labelDate
	 * @param exactMatch
	 * @param outcome
	 * @param mdp
	 * @param extractedText
	 */
	public void processSrlcExtractedTextForAeMatch(int appNumber, String aeType, Date labelDate, boolean exactMatch,
			Outcome outcome, MedDRAProcessor mdp, String extractedText) {

		// Convert app number to pseudo-zip id
		String zipFile = "NDA0" + appNumber;

		// Lower case the text prior to matching
		extractedText = extractedText.toLowerCase().trim();

		List<String> sentences = getSentences(extractedText);
		HashMap<String, List<String>> uniqueAEs = new HashMap<>();
		for (String sentence : sentences) {

			// Split on weird characters
			sentence = splitOnSpecialCharacters(sentence);

			Map<String, List<String>> aeTerms = mdp.processText(aeType, sentence, exactMatch);
			uniqueAEs.putAll(aeTerms);

		}

		for (Map.Entry<String, List<String>> entry : uniqueAEs.entrySet()) {

			// Add each atom to the outcome
			for (String aui : entry.getValue()) {
				if (umls.getMedDRA().containsKey(aui) == true) {
					Atom mdr = umls.getMedDRA().get(aui);
					if (mdr != null) {
						outcome.addCode(zipFile, mdr, labelDate);

					} else {
						Logger.log("Error: Missing meddra code: " + aui);
					}

					/** Enhanced mapping below to expand our map reach **/
					if (mdr.getTty().contentEquals("LLT") || mdr.getTty().contentEquals("MTH_LT")) {
						// Insure the corresponding PT code is added
						// if they have different CUI values
						String ptCode = mdr.getPtCode();

						// This could point to a MedDRA code that does not exist
						// due to incorrect semantic type
						Atom mdrPtCode = umls.getMeddraPtCode(ptCode);
						if (mdrPtCode != null) {
							outcome.addCode(zipFile, mdrPtCode, labelDate);
						}
					}

				} else {
					Logger.log("Missing AUI: " + aui);
				}

			}
		}
	}

	/**
	 * Process single extracted text component
	 * 
	 * @param guid
	 * @param aeType
	 * @param labelDate
	 * @param exactMatch
	 * @param outcome
	 * @param mdp
	 * @param extractedText
	 */
	public void processExtractedTextForAeMatch(String guid, String aeType, Date labelDate, boolean exactMatch,
			Outcome outcome, MedDRAProcessor mdp, String extractedText) {

		// Lower case the text prior to matching
		extractedText = extractedText.toLowerCase().trim();

		List<String> sentences = getSentences(extractedText);
		HashMap<String, List<String>> uniqueAEs = new HashMap<>();
		for (String sentence : sentences) {

			// Split on weird characters
			sentence = splitOnSpecialCharacters(sentence);

			Map<String, List<String>> aeTerms = mdp.processText(aeType, sentence, exactMatch);
			uniqueAEs.putAll(aeTerms);

		}

		for (Map.Entry<String, List<String>> entry : uniqueAEs.entrySet()) {

			// Add each atom to the outcome
			for (String aui : entry.getValue()) {
				// Logger.log("Adding AUI: " + aui);
				if (umls.getMedDRA().containsKey(aui) == true) {
					Atom mdr = umls.getMedDRA().get(aui);
					if (mdr != null) {
						outcome.addCode(guid, mdr, labelDate);

					} else {
						Logger.log("Error: Missing meddra code: " + aui);
					}

					/** Enhanced mapping below to expand our map reach **/
					if (mdr.getTty().contentEquals("LLT") || mdr.getTty().contentEquals("MTH_LT")) {
						// Insure the corresponding PT code is added
						// if they have different CUI values
						String ptCode = mdr.getPtCode();

						// This could point to a MedDRA code that does not exist
						// due to incorrect semantic type
						Atom mdrPtCode = umls.getMeddraPtCode(ptCode);
						if (mdrPtCode != null) {
							outcome.addCode(guid, mdrPtCode, labelDate);
						}
					}

				} else {
					Logger.log("Missing AUI: " + aui);
				}
			}
		}

	}

	private static String splitOnSpecialCharacters(String text) {
		// Split on bullet points, dashes, and other non-alphanumeric characters
		String regex = "[^a-zA-Z0-9\\s-]+"; // Regex to match any non-alphanumeric character except apostrophe and
											// hyphen
		String[] parts = text.split(regex);

		StringBuilder result = new StringBuilder();
		for (String part : parts) {
			// Trim and only add non-empty strings
			String cleanPart = part.trim();
			if (!cleanPart.isEmpty()) {
				result.append(cleanPart);
				result.append(" ");
			}
		}
		return result.toString();
	}

	/**
	 * Use the OpenNLP sentence detector to break up a piece of text into a list of
	 * sentences. Thread-safe: detector is ThreadLocal and regex patterns are
	 * precompiled.
	 */
	private List<String> getSentences(String text) {
		if (text == null || text.isEmpty())
			return java.util.Collections.emptyList();

		final opennlp.tools.sentdetect.SentenceDetectorME detector = SENT_DETECTOR.get();

		try {
			final String[] sentences = detector.sentDetect(text);
			final List<String> results = new ArrayList<>(sentences.length);

			for (String s : sentences) {
				try {
					// collapse whitespace (incl. non-breaking space), then trim
					String cleaned = WS_NBSP.matcher(s).replaceAll(" ").trim();
					if (cleaned.isEmpty())
						continue;

					// remove percent-in-parens like "(12.3%)" or "(5%)"
					cleaned = PCT_PAREN.matcher(cleaned).replaceAll(" ").trim();
					if (cleaned.isEmpty())
						continue;

					// normalize any double spaces created by the previous replacement
					cleaned = WS_NBSP.matcher(cleaned).replaceAll(" ").trim();
					if (!cleaned.isEmpty())
						results.add(cleaned);

				} catch (Exception perSentence) {
					Logger.log("SRLC sentence clean failure: " + perSentence.getMessage());
					// continue with the next sentence
				}
			}
			return results;

		} catch (Exception e) {
			Logger.log("Error in getSentences: " + e.toString());
			Logger.log("Original text (truncated): " + (text.length() > 512 ? text.substring(0, 512) + "..." : text));
			return java.util.Collections.emptyList();
		}
	}

	// Implement NamespaceContext to provide missing namespaces dynamically
	private static class MyNamespaceContext implements NamespaceContext {

		@Override
		public String getNamespaceURI(String prefix) {
			if (prefix == null)
				throw new NullPointerException("Null prefix");
			else if ("xsi".equals(prefix))
				return XMLConstants.W3C_XML_SCHEMA_INSTANCE_NS_URI;
			else if ("xml".equals(prefix))
				return XMLConstants.XML_NS_URI;
			else
				return XMLConstants.NULL_NS_URI;
		}

		@Override
		public String getPrefix(String namespaceURI) {
			throw new UnsupportedOperationException();
		}

		@Override
		public Iterator<String> getPrefixes(String namespaceURI) {
			throw new UnsupportedOperationException();
		}
	}

}
