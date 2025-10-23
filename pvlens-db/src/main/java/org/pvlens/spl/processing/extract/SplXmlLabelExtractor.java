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

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.lang3.StringUtils;
import org.jsoup.Jsoup;
import org.jsoup.select.Elements;
import org.pvlens.spl.conf.ConfigLoader;
import org.pvlens.spl.om.Outcome;
import org.pvlens.spl.om.SplDrug;
import org.pvlens.spl.processing.persist.SqlWriters;
import org.pvlens.spl.processing.support.Dates;
import org.pvlens.spl.umls.Atom;
import org.pvlens.spl.umls.UmlsLoader;
import org.pvlens.spl.util.Logger;
import org.pvlens.spl.util.MedDRAProcessor;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;

/**
 * Parses SPL XML, extracts key dates and section text, and populates exact/NLP
 * outcomes for IND / AE / BLACKBOX. Logic remains 1:1 with the previous version,
 * with comment cleanups and minor robustness improvements.
 */
public class SplXmlLabelExtractor {

	// LOINC section codes
	private static final String IND_CODE = "34067-9";
	private static final String AE_CODE  = "34084-4";
	private static final String BOX_CODE = "34066-1";

	// ANSI-portable control char filter (keep LF/CR/TAB only)
	private static final Pattern NON_PORTABLE_CTRLS =
			Pattern.compile("[\\p{Cntrl}&&[^\\n\\r\\t]]");

	// DB timestamp format (thread-safe)
	private static final DateTimeFormatter DB_FMT =
			DateTimeFormatter.ofPattern(org.pvlens.spl.processing.support.Dates.DB_FMT, Locale.ROOT);

	private static final int VALID_START_YEAR = 1910;
	private static final int VALID_END_YEAR   = ZonedDateTime.now(ZoneOffset.UTC).getYear() + 1;

	// Truncation guard for SQL text columns
	private static final int MAX_TEXT_FIELD_LENGTH = 15800;

	private final UmlsLoader umls;
	private final ConfigLoader cfg;

	// Model resources
	private static final String SENT_MODEL_PATH = "models/en-sent.bin";
	private static final SentenceModel SENTENCE_MODEL = loadSentModel();
	private static final ThreadLocal<SentenceDetectorME> TL_SENT =
			ThreadLocal.withInitial(() -> new SentenceDetectorME(SENTENCE_MODEL));

	// Text cleaners
	private static final Pattern SPACE_RUNS = Pattern.compile("[\\s\\u00A0]+");
	// Match "(5%)" and "(12.5%)"
	private static final Pattern PCT_PAREN  = Pattern.compile("\\(\\d+(?:\\.\\d+)?%\\)");

	public SplXmlLabelExtractor() {
		this.umls = UmlsLoader.getInstance();
		this.cfg  = new ConfigLoader();
	}

	/** Row holder for offline SQL text artifacts. */
	public static final class SplTextRow {
		public final String guid;
		public final String labelDate; // yyyy-MM-dd HH:mm:ss
		public final String text;
		public SplTextRow(String guid, String labelDate, String text) {
			this.guid = guid;
			this.labelDate = labelDate;
			this.text = text;
		}
	}

	private static SentenceModel loadSentModel() {
		try (InputStream in = tryOpen(SENT_MODEL_PATH)) {
			return (in == null) ? null : new SentenceModel(in);
		} catch (Exception e) {
			return null;
		}
	}

	/** Try file system first, then classpath. */
	private static InputStream tryOpen(String path) {
		// Filesystem
		try {
			Path p = Paths.get(path);
			if (Files.exists(p)) return new FileInputStream(p.toFile());
		} catch (Exception ignore) {}
		// Classpath
		try {
			InputStream in = SplXmlLabelExtractor.class.getClassLoader().getResourceAsStream(path);
			if (in != null) return in;
		} catch (Exception ignore) {}
		return null;
	}

	/** Thread-local, hardened DOM builder (XXE / DTD disabled). */
	private static final ThreadLocal<DocumentBuilder> TL_DOM = ThreadLocal.withInitial(() -> {
		try {
			DocumentBuilderFactory f = DocumentBuilderFactory.newInstance();
			f.setNamespaceAware(true);
			f.setValidating(false);
			f.setXIncludeAware(false);
			f.setExpandEntityReferences(false);

			// XXE / DTD / external entities OFF
			f.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
			f.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
			f.setFeature("http://xml.org/sax/features/external-general-entities", false);
			f.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
			f.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);

			// Best-effort external access lockdown
			try {
				f.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
				f.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
			} catch (Throwable ignore) {}

			return f.newDocumentBuilder();
		} catch (Exception e) {
			throw new RuntimeException("Failed to init secure XML builder", e);
		}
	});

	/**
	 * Process all XML files for a GUID: extract, run exact and NLP matches, and persist text.
	 */
	public void processGuid(
			SplDrug spl,
			ConcurrentMap<String, Integer> xmlIndPass,
			ConcurrentMap<String, Integer> xmlAePass,
			ConcurrentMap<String, Integer> xmlBoxPass,
			boolean debug,
			SqlWriters writers) {

		if (spl.getGuid() == null) {
			Logger.log("No GUID found!");
			spl.setSave(false);
			return;
		}
		if (spl.getXmlFiles().isEmpty()) {
			Logger.log("Error - no XML files defined for GUID: " + spl.getGuid());
			spl.setSave(false);
			return;
		}

		for (String rel : spl.getXmlFiles().keySet()) {
			if (Boolean.TRUE.equals(spl.getXmlFiles().get(rel))) {
				Logger.debug("Skipping XML already processed...");
				continue;
			}

			String xmlFile = rel;
			Path target = Paths.get(xmlFile).toAbsolutePath().normalize();
			if (!Files.exists(target)) continue;

			// Derive source type (path cue)
			String srcType = rel.contains("prescription") ? "prescription"
					: rel.contains("otc") ? "otc"
					: rel.contains("other") ? "other"
					: "";

			// Canonical root guard (supports year subfolders under SPL root)
			Path cfgRoot = Paths.get(cfg.getSplPath()).toAbsolutePath().normalize();     // e.g., /home/painter/spl_archive
			String src   = String.valueOf(srcType);                                      // ensure non-null

			// 1) Must live under the configured SPL archive root
			if (!target.startsWith(cfgRoot)) {
			    Logger.error("Blocked XML outside archive root: " + target);
			    continue;
			}

			// 2) Must contain .../xml_files/<srcType>/... somewhere after the root
			Path relative = cfgRoot.relativize(target);  // e.g., 2016/xml_files/prescription/a8edb191-....xml
			boolean ok = false;
			for (int i = 0; i <= relative.getNameCount() - 3; i++) { // need at least xml_files + srcType + filename
			    if ("xml_files".equals(relative.getName(i).toString())
			            && relative.getName(i + 1).toString().equalsIgnoreCase(src)) {
			        ok = true;
			        break;
			    }
			}

			if (!ok) {
			    Logger.error("Blocked XML outside expected xml_files/" + src + " subtree: " + target);
			    continue;
			}			

			// Size guard (15 MB default)
			try {
				long size = Files.size(target);
				long maxBytes = 15L * 1024 * 1024;
				if (size > maxBytes) {
					Logger.warn("Skipping oversized SPL XML (" + size + " bytes): " + target);
					continue;
				}
			} catch (Exception sizeEx) {
				Logger.warn("Size check failed, skipping: " + target + " — " + sizeEx.getMessage());
				continue;
			}

			try (FileInputStream xmlContentStream = new FileInputStream(xmlFile)) {
				SplDrug tmpSpl = spl.copySplDrug();

				DocumentBuilder b = TL_DOM.get();
				Document doc = b.parse(xmlContentStream);
				doc.getDocumentElement().normalize();

				if ("other".equals(srcType)) {
					String loinc = firstDocumentLoincCode(doc);
					if (!cfg.getOtherLoincFilter().allow(loinc)) {
						continue;
					}
				}

				Date finalApproval = computeApprovalDate(doc);
				if (finalApproval != null) {
					spl.getGuidApprovalDate().put(spl.getGuid(), finalApproval);
				}

				int ndaId = extractNda(doc);
				if (ndaId > 0) spl.setNda(ndaId);

				Date labelDate    = firstNonNull(extractLabelDate(doc));
				Date indDate      = defaultIfNull(extractIndicationDate(doc), labelDate);
				Date aeDate       = defaultIfNull(extractAdverseReactionsDate(doc), labelDate);
				Date blackBoxDate = defaultIfNull(extractBlackBoxDate(doc), labelDate);

				Outcome exactInd = extractAeBlock(spl.getGuid(), doc, IND_CODE, "IND", indDate, true,  xmlIndPass, writers.get("IND_TEXT"));
				String  indText  = removeExact(doc, IND_CODE, "Indications",        exactInd);
				Outcome nlpInd   = nlpOnText(spl.getGuid(), indText, "IND", indDate, false);
				removeDeath(exactInd);
				removeDeath(nlpInd);

				Outcome exactAe  = extractAeBlock(spl.getGuid(), doc, AE_CODE,  "AE", aeDate, true,   xmlAePass,  writers.get("AE_TEXT"));
				String  aeText   = removeExact(doc, AE_CODE,  "Adverse_Reactions", exactAe);
				Outcome nlpAe    = nlpOnText(spl.getGuid(), aeText,  "AE", aeDate,  false);

				Outcome exactBox = extractAeBlock(spl.getGuid(), doc, BOX_CODE, "BLACKBOX", blackBoxDate, true, xmlBoxPass, writers.get("BOX_TEXT"));
				String  boxText  = removeExact(doc, BOX_CODE, "Box", exactBox);
				Outcome nlpBox   = nlpOnText(spl.getGuid(), boxText, "BLACKBOX", blackBoxDate, false);

				// Flags
				exactInd.setExactMatch(true);
				tmpSpl.setExactMatchIndications(exactInd);
				nlpInd.setExactMatch(false);
				tmpSpl.setNlpMatchIndications(nlpInd);

				exactAe.setExactMatch(true);
				tmpSpl.setExactMatchWarnings(exactAe);
				nlpAe.setExactMatch(false);
				tmpSpl.setNlpMatchWarnings(nlpAe);

				exactBox.setExactMatch(true);
				tmpSpl.setExactMatchBlackbox(exactBox);
				nlpBox.setExactMatch(false);
				tmpSpl.setNlpMatchBlackbox(nlpBox);

				tmpSpl.resolveLabeledEvents();

				boolean merged = spl.mergeProductGroup(tmpSpl);
				if (!merged) {
					Logger.error("Failed to merge!");
				}

				// Mark file as processed
				spl.getXmlFiles().put(rel, true);

			} catch (Exception e) {
				Logger.log("Error parsing XML file [" + rel + "]: " + e);
			}
		}
	}

	// ---------- Helpers (kept from prior version; names preserved) ----------

	private static Date defaultIfNull(Date d, Date fallback) { return (d != null) ? d : fallback; }
	private static Date firstNonNull(Date d)                 { return d; }

	/** Returns approval date (title year if present; else earliest effectiveTime). */
	private Date computeApprovalDate(Document doc) throws Exception {
		int year = extractApprovalYear(doc);
		Date titleY = (year >= 1910 && year <= 2100) ? Dates.parseYyyyMMdd(year + "0101") : null;
		if (titleY != null) return titleY;
		Date eff = extractEarliestEffectiveDate(doc);
		return eff;
	}

	private Outcome extractAeBlock(String guid,
	                               Document document,
	                               String sectionCode,
	                               String aeType,
	                               Date labelDate,
	                               boolean exact,
	                               ConcurrentMap<String, Integer> passCounter,
	                               PrintWriter output) {
		Outcome outcome = new Outcome();
		String sectionId;
		String table;

		switch (aeType) {
			case "AE" -> {
				outcome.setWarning(true);
				sectionId = "Adverse_Reactions";
				table = "SPL_AE_TEXT";
			}
			case "BLACKBOX" -> {
				outcome.setBlackbox(true);
				sectionId = "Box";
				table = "SPL_BOX_TEXT";
			}
			case "IND" -> {
				outcome.setIndication(true);
				sectionId = "Indications";
				table = "SPL_IND_TEXT";
			}
			default -> {
				sectionId = "Indications";
				table = "SPL_IND_TEXT";
			}
		}

		try {
			MedDRAProcessor mdp = new MedDRAProcessor(this.umls);
			String extractedText = getSectionText(document, sectionCode, sectionId, passCounter);
			if (StringUtils.isNotEmpty(extractedText)) {
				String sqlSafe = sanitizeForSQLPlainLiteral(extractedText);

				StringBuilder sql = new StringBuilder(256);
				sql.append("INSERT INTO ").append(table).append(" (GUID, LABEL_DATE, SPL_TEXT) VALUES ('")
						.append(sqlQuote(guid)).append("',");
				sql.append((labelDate == null) ? "NULL" : ("'" + sqlQuote(fmt(labelDate)) + "'"));
				sql.append(",'").append(sqlSafe).append("');");

				w(output, sql.toString());
				processExtractedTextForAeMatch(guid, aeType, labelDate, exact, outcome, mdp, extractedText);
			}
		} catch (Exception e) {
			Logger.error("Error processing document (here): [" + aeType + "] " + e.toString());
		}
		return outcome;
	}

	/** Run NLP matching on cleaned text after exact matches were removed. */
	private Outcome nlpOnText(String guid, String cleanedText, String aeType, Date labelDate, boolean exactMatch) {
		Outcome outcome = new Outcome();
		outcome.setWarning(true);

		try {
			MedDRAProcessor mdp = new MedDRAProcessor(umls);
			if (StringUtils.isNotEmpty(cleanedText)) {
				List<String> sentences = getSentences(cleanedText);
				HashMap<String, List<String>> uniqueAEs = new HashMap<>();
				for (String sentence : sentences) {
					sentence = splitOnSpecialCharacters(sentence);
					Map<String, List<String>> aeTerms = mdp.processText(aeType, sentence, exactMatch);
					uniqueAEs.putAll(aeTerms);
				}

				for (Map.Entry<String, List<String>> entry : uniqueAEs.entrySet()) {
					for (String aui : entry.getValue()) {
						if (umls.getMedDRA().containsKey(aui)) {
							Atom mdr = umls.getMedDRA().get(aui);
							outcome.addCode(guid, mdr, labelDate);
						} else {
							Logger.log("Missing AUI: " + aui);
						}
					}
				}
			}
		} catch (Exception e) {
			Logger.error("Error processing cleaned text: " + e.toString());
		}
		return outcome;
	}

	/**
	 * Process extracted section text and add MedDRA codes to the outcome.
	 */
	public void processExtractedTextForAeMatch(String guid,
	                                           String aeType,
	                                           Date labelDate,
	                                           boolean exactMatch,
	                                           Outcome outcome,
	                                           MedDRAProcessor mdp,
	                                           String extractedText) {

		String lower = extractedText.toLowerCase(Locale.ROOT).trim();
		List<String> sentences = getSentences(lower);
		HashMap<String, List<String>> uniqueAEs = new HashMap<>();

		for (String sentence : sentences) {
			String s = splitOnSpecialCharacters(sentence);
			Map<String, List<String>> aeTerms = mdp.processText(aeType, s, exactMatch);
			uniqueAEs.putAll(aeTerms);
		}

		for (Map.Entry<String, List<String>> entry : uniqueAEs.entrySet()) {
			for (String aui : entry.getValue()) {
				if (umls.getMedDRA().containsKey(aui)) {
					Atom mdr = umls.getMedDRA().get(aui);
					if (mdr != null) {
						outcome.addCode(guid, mdr, labelDate);
					} else {
						Logger.log("Error: Missing meddra code: " + aui);
					}
					// Expand LLT/MTH_LT to PT where applicable
					if ("LLT".contentEquals(mdr.getTty()) || "MTH_LT".contentEquals(mdr.getTty())) {
						String ptCode = mdr.getPtCode();
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

	/**
	 * Split on non-alphanumeric characters (keeping hyphens and spaces).
	 * Note: apostrophes are removed by design.
	 */
	private static String splitOnSpecialCharacters(String text) {
		String regex = "[^a-zA-Z0-9\\s-]+";
		String[] parts = text.split(regex);
		StringBuilder result = new StringBuilder();
		for (String part : parts) {
			String cleanPart = part.trim();
			if (!cleanPart.isEmpty()) result.append(cleanPart).append(' ');
		}
		return result.toString().trim();
	}

	/** Sentence detection via OpenNLP, with robust fallbacks. */
	private List<String> getSentences(String text) {
		List<String> out = new ArrayList<>();
		if (text == null || text.isBlank()) return out;

		if (SENTENCE_MODEL == null) {
			for (String s : text.split("(?<=[.!?])\\s+")) {
				String cleaned = SPACE_RUNS.matcher(s).replaceAll(" ").trim();
				if (!cleaned.isEmpty()) out.add(cleaned);
			}
			return out;
		}

		try {
			SentenceDetectorME sd = TL_SENT.get();
			String[] sents = sd.sentDetect(text);
			for (String s : sents) {
				String cleaned = SPACE_RUNS.matcher(s).replaceAll(" ").trim();
				if (!cleaned.isEmpty()) {
					cleaned = PCT_PAREN.matcher(cleaned).replaceAll(" ");
					cleaned = SPACE_RUNS.matcher(cleaned).replaceAll(" ").trim();
					if (!cleaned.isEmpty()) out.add(cleaned);
				}
			}
		} catch (Throwable t) {
			Logger.log("Sentence detector hiccup; rebuilding for this thread: " + t);
			TL_SENT.remove();
			try {
				String[] sents = TL_SENT.get().sentDetect(text);
				for (String s : sents) {
					String cleaned = SPACE_RUNS.matcher(s).replaceAll(" ").trim();
					if (!cleaned.isEmpty()) {
						cleaned = PCT_PAREN.matcher(cleaned).replaceAll(" ");
						cleaned = SPACE_RUNS.matcher(cleaned).replaceAll(" ").trim();
						if (!cleaned.isEmpty()) out.add(cleaned);
					}
				}
			} catch (Throwable t2) {
				for (String s : text.split("(?<=[.!?])\\s+")) {
					String cleaned = SPACE_RUNS.matcher(s).replaceAll(" ").trim();
					if (!cleaned.isEmpty()) out.add(cleaned);
				}
			}
		}
		return out;
	}

	/**
	 * Attempt primary/secondary/tertiary section extraction and track which pass succeeded.
	 */
	public static String getSectionText(Document document,
	                                    String sectionCode,
	                                    String sectionId,
	                                    ConcurrentMap<String, Integer> globalPassTracker) {

		String extractedText = extractTextFromSection(document, sectionCode, sectionId);

		if (StringUtils.isEmpty(extractedText)) {
			extractedText = secondaryExtractTextFromSection(document, sectionCode);
			if (StringUtils.isEmpty(extractedText)) {
				extractedText = tertiaryExtractTextFromSection(document, sectionCode);
				if (StringUtils.isNotEmpty(extractedText) && globalPassTracker != null) {
					globalPassTracker.merge("3", 1, Integer::sum);
				}
			} else if (globalPassTracker != null) {
				globalPassTracker.merge("2", 1, Integer::sum);
			}
		} else if (globalPassTracker != null) {
			globalPassTracker.merge("1", 1, Integer::sum);
		}
		return extractedText;
	}

	/**
	 * Extract text from the section matching the given LOINC code and section ID.
	 */
	private static String extractTextFromSection(Document document, String sectionCode, String sectionId) {
		Element root = document.getDocumentElement();
		return findSectionWithCodeAndIdInComponent(root, sectionCode, sectionId);
	}

	/**
	 * Remove exact-match terms from the section text to avoid duplicates in NLP pass.
	 */
	private String removeExact(Document document, String sectionCode, String sectionId, Outcome outcome) {
		String extractedText = extractTextFromSection(document, sectionCode, sectionId);
		if (StringUtils.isEmpty(extractedText)) {
			extractedText = secondaryExtractTextFromSection(document, sectionCode);
			if (StringUtils.isEmpty(extractedText)) {
				extractedText = tertiaryExtractTextFromSection(document, sectionCode);
			}
		}

		if (StringUtils.isNotEmpty(extractedText)) {
			for (Atom exactMatchAtom : outcome.getCodes()) {
				String term = exactMatchAtom.getTerm();
				extractedText = extractedText.replaceAll("\\b" + Pattern.quote(term) + "\\b", "");
			}
		}
		return extractedText.trim();
	}

	/** Parse approval year from document title (e.g., "Initial U.S. Approval: 2012"). */
	private int extractApprovalYear(Document document) {
		int year = -1;
		NodeList documentTitle = document.getElementsByTagName("title");
		if (documentTitle.getLength() > 0) {
			Node titleNode = documentTitle.item(0);
			String titleText = titleNode.getTextContent();
			if (StringUtils.isNotEmpty(titleText)) {
				year = getApprovalYear(titleText);
			}
		}
		return year;
	}

	private int getApprovalYear(String inputText) {
		inputText = inputText.replaceAll("\\s+", " ");
		Pattern r = Pattern.compile("Initial U\\.S\\. Approval: (\\d{4})");
		Matcher m = r.matcher(inputText);
		return m.find() ? Integer.parseInt(m.group(1)) : -1;
	}

	/** Remove "death" terms from outcomes. */
	private void removeDeath(Outcome outcome) {
		Set<Atom> cleanAtoms = new HashSet<>();
		HashMap<String, Date> firstAdded = new HashMap<>();
		for (Atom atom : outcome.getCodes()) {
			if (!atom.getTerm().toLowerCase(Locale.ROOT).contains("death")) {
				cleanAtoms.add(atom);
				Date addDte = outcome.getFirstAdded().get(atom.getAui());
				firstAdded.put(atom.getAui(), addDte);
			}
		}
		outcome.setCodes(cleanAtoms);
		outcome.setFirstAdded(firstAdded);
	}

	/** Scan for earliest valid effective date anywhere in the document. */
	private Date extractEarliestEffectiveDate(Document document) {
		try {
			List<String> candidates = new ArrayList<>();
			NodeList effs = document.getElementsByTagName("effectiveTime");
			for (int i = 0; i < effs.getLength(); i++) {
				Node n = effs.item(i);

				// effectiveTime/@value
				if (n.hasAttributes()) {
					Node val = n.getAttributes().getNamedItem("value");
					if (val != null) {
						String norm = Dates.normalizeToYyyyMMdd(val.getNodeValue());
						if (norm != null) candidates.add(norm);
					}
				}

				// <effectiveTime><low value="..."/></effectiveTime>
				if (n.getNodeType() == Node.ELEMENT_NODE) {
					Element e = (Element) n;
					NodeList lows = e.getElementsByTagName("low");
					for (int j = 0; j < lows.getLength(); j++) {
						Node low = lows.item(j);
						if (low != null && low.hasAttributes()) {
							Node val = low.getAttributes().getNamedItem("value");
							if (val != null) {
								String norm = Dates.normalizeToYyyyMMdd(val.getNodeValue());
								if (norm != null) candidates.add(norm);
							}
						}
					}
				}
			}

			Date earliest = null;
			for (String c : candidates) {
				try {
					int year = Integer.parseInt(c.substring(0, 4));
					if (year < VALID_START_YEAR || year > VALID_END_YEAR) continue;
					Date d = Dates.parseYyyyMMdd(c);
					if (earliest == null || d.before(earliest)) earliest = d;
				} catch (Exception ignore) {}
			}
			return earliest;
		} catch (Exception e) {
			Logger.log("Error scanning earliest effective date: " + e.getMessage());
			return null;
		}
	}

	/** Extract NDA/BLA tracking number from id[@root='2.16.840.1.113883.3.150'] */
	private int extractNda(Document document) {
		int ndaId = -1;
		String ndaValue = "";
		String rootValue = "2.16.840.1.113883.3.150";

		NodeList nodeList = document.getElementsByTagName("id");
		for (int i = 0; i < nodeList.getLength(); i++) {
			Element idElement = (Element) nodeList.item(i);
			if (rootValue.equals(idElement.getAttribute("root"))) {
				ndaValue = idElement.getAttribute("extension");
				break;
			}
		}
		if (ndaValue.contains("NDA") || ndaValue.contains("BLA")) {
			try {
				if (ndaValue.contains("NDA")) ndaId = Integer.parseInt(ndaValue.replace("NDA", "").trim());
				if (ndaValue.contains("BLA")) ndaId = Integer.parseInt(ndaValue.replace("BLA", "").trim());
			} catch (Exception ignore) {}
		}
		return ndaId;
	}

	/** Label date from top-level effectiveTime/@value in yyyyMMdd if present and sane. */
	private Date extractLabelDate(Document document) throws Exception {
		NodeList list = document.getElementsByTagName("effectiveTime");
		if (list.getLength() == 0) {
			Logger.log("No date found in effectiveTime element.");
			return null;
		}

		Node dateNode = list.item(0);
		NamedNodeMap attributes = dateNode.getAttributes();
		Node codeAttr = attributes.getNamedItem("value");
		if (codeAttr == null) return null;

		String dateString = codeAttr.getNodeValue().trim();
		if (StringUtils.isEmpty(dateString)) return null;

		try {
			if (dateString.length() == 8 && dateString.matches("\\d{8}")) {
				int year = Integer.parseInt(dateString.substring(0, 4));
				if (year >= VALID_START_YEAR && year <= VALID_END_YEAR) {
					return Dates.parseYyyyMMdd(dateString);
				}
			}
		} catch (NumberFormatException e) {
			Logger.log("NumberFormatException while parsing date: " + dateString);
		}
		return null;
	}

	private Date extractIndicationDate(Document document)       { return extractEffectiveDate(document, IND_CODE); }
	private Date extractBlackBoxDate(Document document)         { return extractEffectiveDate(document, BOX_CODE); }
	private Date extractAdverseReactionsDate(Document document) { return extractEffectiveDate(document, AE_CODE); }

	/**
	 * Extract the effective date from a specific section code (if present).
	 */
	private Date extractEffectiveDate(Document document, String sectionCode) {
		try {
			NodeList sectionList = document.getElementsByTagName("section");
			for (int i = 0; i < sectionList.getLength(); i++) {
				Node sectionNode = sectionList.item(i);
				if (sectionNode.getNodeType() != Node.ELEMENT_NODE) continue;

				Element sectionElement = (Element) sectionNode;
				NodeList codeList = sectionElement.getElementsByTagName("code");
				if (codeList.getLength() == 0) continue;

				Element codeElement = (Element) codeList.item(0);
				if (!sectionCode.equals(codeElement.getAttribute("code"))) continue;

				NodeList effectiveTimeList = sectionElement.getElementsByTagName("effectiveTime");
				if (effectiveTimeList.getLength() == 0) continue;

				Node dateNode = effectiveTimeList.item(0);
				NamedNodeMap attributes = dateNode.getAttributes();
				Node codeAttr = attributes.getNamedItem("value");
				if (codeAttr != null) {
					String dateString = codeAttr.getNodeValue();
					if (StringUtils.isNotEmpty(dateString)) return Dates.parseYyyyMMdd(dateString);
				}
			}
		} catch (Exception e) {
			Logger.log("Error extracting date for section code: " + sectionCode + " - " + e.getMessage());
		}
		return null;
	}

	/**
	 * Search for a section by code + section ID and extract its text via HTML-aware parsing.
	 */
	private static String findSectionWithCodeAndIdInComponent(Element element, String sectionCode, String sectionId) {
		NodeList componentNodes = element.getElementsByTagName("component");

		for (int i = 0; i < componentNodes.getLength(); i++) {
			Node componentNode = componentNodes.item(i);
			if (componentNode.getNodeType() != Node.ELEMENT_NODE) continue;

			Element componentElement = (Element) componentNode;
			NodeList sectionNodes = componentElement.getElementsByTagName("section");
			for (int j = 0; j < sectionNodes.getLength(); j++) {
				Node sectionNode = sectionNodes.item(j);
				if (sectionNode.getNodeType() != Node.ELEMENT_NODE) continue;

				Element sectionElement = (Element) sectionNode;
				NodeList codeNodes = sectionElement.getElementsByTagName("code");
				for (int k = 0; k < codeNodes.getLength(); k++) {
					Element codeElement = (Element) codeNodes.item(k);
					if (sectionCode.equals(codeElement.getAttribute("code"))
							&& sectionId.equalsIgnoreCase(sectionElement.getAttribute("ID"))) {
						return extractHtmlText(sectionElement);
					}
				}
			}
		}
		return "";
	}

	private static String secondaryExtractTextFromSection(Document document, String sectionCode) {
		Element root = document.getDocumentElement();
		return alternateTextExtractionSearch(root, sectionCode);
	}

	private static String alternateTextExtractionSearch(Element element, String sectionCode) {
		NodeList componentNodes = element.getElementsByTagName("component");

		for (int i = 0; i < componentNodes.getLength(); i++) {
			Node componentNode = componentNodes.item(i);
			if (componentNode.getNodeType() != Node.ELEMENT_NODE) continue;

			Element componentElement = (Element) componentNode;
			NodeList sectionNodes = componentElement.getElementsByTagName("section");
			for (int j = 0; j < sectionNodes.getLength(); j++) {
				Node sectionNode = sectionNodes.item(j);
				if (sectionNode.getNodeType() != Node.ELEMENT_NODE) continue;

				Element sectionElement = (Element) sectionNode;
				NodeList codeNodes = sectionElement.getElementsByTagName("code");
				for (int k = 0; k < codeNodes.getLength(); k++) {
					Element codeElement = (Element) codeNodes.item(k);
					if (sectionCode.equals(codeElement.getAttribute("code"))) {
						return extractHtmlText(sectionElement);
					}
				}
			}
		}
		return "";
	}

	private static String tertiaryExtractTextFromSection(Document document, String sectionCode) {
		NodeList componentNodes = document.getElementsByTagName("component");

		for (int i = 0; i < componentNodes.getLength(); i++) {
			Node componentNode = componentNodes.item(i);
			if (componentNode.getNodeType() != Node.ELEMENT_NODE) continue;

			Element componentElement = (Element) componentNode;
			NodeList sectionNodes = componentElement.getElementsByTagName("section");
			for (int j = 0; j < sectionNodes.getLength(); j++) {
				Node sectionNode = sectionNodes.item(j);
				if (sectionNode.getNodeType() != Node.ELEMENT_NODE) continue;

				Element sectionElement = (Element) sectionNode;
				NodeList codeNodes = sectionElement.getElementsByTagName("code");
				for (int k = 0; k < codeNodes.getLength(); k++) {
					Element codeElement = (Element) codeNodes.item(k);
					if (sectionCode.equals(codeElement.getAttribute("code"))) {
						return extractHtmlText(sectionElement);
					}
				}
			}
		}
		return "";
	}

	/** Extract plain text recursively (fallback path). */
	private static String extractPlainText(Element element) {
		StringBuilder textContent = new StringBuilder();
		NodeList childNodes = element.getChildNodes();

		for (int i = 0; i < childNodes.getLength(); i++) {
			Node node = childNodes.item(i);
			if (node.getNodeType() == Node.TEXT_NODE) {
				textContent.append(node.getTextContent().trim()).append(' ');
			} else if (node.getNodeType() == Node.ELEMENT_NODE) {
				textContent.append(extractPlainText((Element) node));
			}
		}
		return textContent.toString().trim();
	}

	/**
	 * Get inner XML of an element, preserving tags (Transformer hardened).
	 */
	private static String getInnerXml(Element element) {
		try {
			TransformerFactory factory = TransformerFactory.newInstance();
			try {
				factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
				factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
				factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_STYLESHEET, "");
			} catch (Throwable ignore) {}

			Transformer transformer = factory.newTransformer();
			transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
			transformer.setOutputProperty(OutputKeys.INDENT, "yes");
			transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");

			DOMSource source = new DOMSource(element);
			source.setNode(element);

			StringWriter writer = new StringWriter();
			StreamResult result = new StreamResult(writer);
			transformer.transform(source, result);
			return writer.toString();

		} catch (TransformerException e) {
			return extractPlainText(element);
		}
	}

	/**
	 * Extract text from a section while handling paragraph/table markup.
	 * Avoids duplication by not re-appending body text already captured from
	 * paragraphs and tables.
	 */
	private static String extractHtmlText(Element sectionElement) {
		String innerXml = getInnerXml(sectionElement);
		if (innerXml == null || innerXml.isEmpty()) return "";

		try {
			org.jsoup.nodes.Document jsoupDoc = Jsoup.parse(innerXml);

			StringBuilder extractedText = new StringBuilder();

			// Paragraphs
			Elements paragraphs = jsoupDoc.select("paragraph");
			for (org.jsoup.nodes.Element p : paragraphs) {
				extractedText.append(p.text()).append("\n");
			}

			// Tables
			Elements tables = jsoupDoc.select("table");
			for (org.jsoup.nodes.Element table : tables) {
				Elements rows = table.select("tr");
				for (org.jsoup.nodes.Element row : rows) {
					Elements cells = row.select("td");
					for (org.jsoup.nodes.Element cell : cells) {
						extractedText.append(cell.text()).append(' ');
					}
					extractedText.append("\n");
				}
			}

			// Remove elements we've already serialized, then add any residual text once
			jsoupDoc.select("paragraph,table").remove();
			String residual = jsoupDoc.body() != null ? jsoupDoc.body().text() : "";
			if (StringUtils.isNotBlank(residual)) {
				extractedText.append(residual);
			}

			return extractedText.toString().trim();

		} catch (Exception e) {
			return extractPlainText(sectionElement);
		}
	}

	/**
	 * Produce a safe single-quoted SQL literal body (no surrounding quotes).
	 * - Normalizes newlines
	 * - Strips non-portable control characters
	 * - Strips unpaired surrogates
	 * - Escapes quotes and collapses over-escapes
	 * - Truncates safely to MAX_TEXT_FIELD_LENGTH
	 */
	public static String sanitizeForSQLPlainLiteral(String input) {
		if (input == null) return "";

		// 1) Normalize newlines: CRLF -> LF, CR -> LF (keep only '\n')
		String s = input.replace("\r\n", "\n").replace("\r", "\n");

		// 2) Strip odd control chars (keep \n and \t)
		s = NON_PORTABLE_CTRLS.matcher(s).replaceAll("");

		// 3) Drop any unpaired surrogates
		s = stripUnpairedSurrogates(s);

		// 4) If RAW ends with odd run of quotes, drop one
		int tailQ = 0;
		for (int i = s.length() - 1; i >= 0 && s.charAt(i) == '\''; i--) tailQ++;
		if (tailQ % 2 == 1) s = s.substring(0, s.length() - 1);

		// 5) ANSI-escape quotes and collapse '''' -> ''
		s = s.replace("'", "''");
		String prev;
		do {
			prev = s;
			s = s.replace("''''", "''");
		} while (!s.equals(prev));

		// 6) Safe truncate after escaping/collapse
		if (s.length() > MAX_TEXT_FIELD_LENGTH) {
			s = s.substring(0, MAX_TEXT_FIELD_LENGTH);

			// orphan trailing quote?
			if (!s.isEmpty() && s.charAt(s.length() - 1) == '\''
					&& (s.length() == 1 || s.charAt(s.length() - 2) != '\'')) {
				s = s.substring(0, s.length() - 1);
			}
			// strip unpaired surrogates again after truncation
			s = stripUnpairedSurrogates(s);
		}

		// 7) Final guards
		if (!s.isEmpty() && s.charAt(s.length() - 1) == '\''
				&& (s.length() == 1 || s.charAt(s.length() - 2) != '\'')) {
			s = s.substring(0, s.length() - 1);
		}
		// Strip trailing backslashes
		s = s.replaceAll("\\\\+$", "");

		return s;
	}

	/** Remove any unpaired surrogate code units from the string. */
	private static String stripUnpairedSurrogates(String s) {
		if (s == null || s.isEmpty()) return s;
		StringBuilder b = new StringBuilder(s.length());
		for (int i = 0; i < s.length();) {
			char ch = s.charAt(i);
			if (Character.isHighSurrogate(ch)) {
				if (i + 1 < s.length() && Character.isLowSurrogate(s.charAt(i + 1))) {
					b.append(ch).append(s.charAt(i + 1));
					i += 2;
				} else {
					i += 1; // drop lone high surrogate
				}
			} else if (Character.isLowSurrogate(ch)) {
				i += 1;   // drop lone low surrogate
			} else {
				b.append(ch);
				i += 1;
			}
		}
		return b.toString();
	}

	/** Double single quotes for SQL string literals (identifiers like GUID). */
	private static String sqlQuote(String s) {
		return (s == null) ? "" : s.replace("'", "''");
	}

	/** First document-level LOINC code (codeSystem 2.16.840.1.113883.6.1), if any. */
	private static String firstDocumentLoincCode(Document doc) {
		NodeList codes = doc.getElementsByTagName("code");
		for (int i = 0; i < codes.getLength(); i++) {
			Node n = codes.item(i);
			if (n.getNodeType() != Node.ELEMENT_NODE) continue;
			Element e = (Element) n;
			if ("2.16.840.1.113883.6.1".equals(e.getAttribute("codeSystem"))) {
				String code = e.getAttribute("code");
				if (code != null && !code.isBlank()) return code.trim();
			}
		}
		return null;
	}

	private static String fmt(Date d) {
		if (d == null) return null;
		ZonedDateTime zdt = d.toInstant().atZone(ZoneOffset.UTC);
		return DB_FMT.format(zdt);
	}

	/** Thread-safe writer helper (per-writer lock). */
	private void w(PrintWriter pw, String s) {
		synchronized (pw) {
			pw.println(s);
		}
	}
}
