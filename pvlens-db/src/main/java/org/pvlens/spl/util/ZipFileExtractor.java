package org.pvlens.spl.util;

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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.pvlens.spl.conf.ConfigLoader;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Extracts XML SPL documents from ZIP archives and writes two CSV maps: -
 * (ZipFileName, XmlFileName, SourceType) - (ZipFileName,
 * ManufactureProductName)
 *
 * Incremental by default; optional testMode restricts work to ZIPs already
 * listed in the CSV maps and (re)extracts their XMLs if missing.
 *
 * Author: Jeffery Painter Updated: 2025-08-27 (add testMode)
 */
public class ZipFileExtractor {

	// Instance paths
	private final Path splDirPath;
	private final Path xmlOutputDirPath; // extracted XML root
	private final Path xmlMapPath;
	private final Path prodMapPath;

	// CSV headers
	private static final String[] XML_MAP_HEADER = { "ZipFileName", "XmlFileName", "SourceType" };
	private static final String[] PROD_MAP_HEADER = { "ZipFileName", "ManufactureProductName" };

	public ZipFileExtractor(Path myPath) {
		ConfigLoader cfg = new ConfigLoader();
		this.splDirPath = myPath;
		this.xmlOutputDirPath = splDirPath.resolve("xml_files");
		this.xmlMapPath = splDirPath.resolve(cfg.getZipToXmlMapFilename());
		this.prodMapPath = splDirPath.resolve(cfg.getSplProductLabelMap());
	}

	public ZipFileExtractor(String path) {
		ConfigLoader cfg = new ConfigLoader();
		this.splDirPath = Paths.get(path);
		this.xmlOutputDirPath = splDirPath.resolve("xml_files");
		this.xmlMapPath = splDirPath.resolve(cfg.getZipToXmlMapFilename());
		this.prodMapPath = splDirPath.resolve(cfg.getSplProductLabelMap());
	}

	// --- Configuration --------------------------------------------------------
	/** IO is the bottleneck; allow multiple threads per core to overlap disk. */
	private static final int THREAD_COUNT = Math.max(4, Runtime.getRuntime().availableProcessors() * 2);

	/** Progress log cadence (zip files). */
	private static final int PROGRESS_EVERY = 100;

	// --- XML parser (thread-local, secure) -----------------------------------
	private static final ThreadLocal<DocumentBuilder> TL_DOCUMENT_BUILDER = ThreadLocal.withInitial(() -> {
		try {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			dbf.setNamespaceAware(true);
			dbf.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
			dbf.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
			dbf.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
			dbf.setExpandEntityReferences(false);
			return dbf.newDocumentBuilder();
		} catch (Exception e) {
			throw new IllegalStateException("Failed to initialize secure XML parser", e);
		}
	});

	// --- Public entry points --------------------------------------------------

	/** Backward-compatible default: incremental, scans for new ZIPs. */
	@SuppressWarnings("resource")
	public void extractXmlFiles() {
		extractXmlFiles(false);
	}

	/**
	 * If testMode=true: - Do NOT look for new ZIPs beyond those already in
	 * PRODUCT_LABEL_MAP.csv (and any already listed in SPL_ZIP_XML_MAP.csv). - For
	 * those ZIPs only, ensure XMLs exist: (re)extract and append missing CSV rows.
	 */
	@SuppressWarnings("resource")
	public void extractXmlFiles(boolean testMode) {

		try {
			if (!Files.isDirectory(splDirPath)) {
				Logger.error("SPL directory does not exist: " + splDirPath);
				return;
			}

			if (Files.notExists(xmlOutputDirPath)) {
				Files.createDirectories(xmlOutputDirPath);
			}

			// Build scan targets (subfolders if present; else legacy root)
			record ScanTarget(Path dir, String sourceType) {
			}
			java.util.ArrayList<ScanTarget> targets = new java.util.ArrayList<>();
			Path presDir = splDirPath.resolve("prescription");
			Path otherDir = splDirPath.resolve("other");
			Path otcDir = splDirPath.resolve("otc");
			boolean presExists = Files.isDirectory(presDir);
			boolean otherExists = Files.isDirectory(otherDir);
			boolean otcExists = Files.isDirectory(otcDir);
			if (presExists)
				targets.add(new ScanTarget(presDir, "prescription"));
			if (otherExists)
				targets.add(new ScanTarget(otherDir, "other"));
			if (otcExists)
				targets.add(new ScanTarget(otcDir, "otc"));
			if (targets.isEmpty())
				targets.add(new ScanTarget(splDirPath, "prescription"));

			// Trackers & existing state
			boolean xmlMapExists = Files.exists(xmlMapPath);
			boolean prodMapExists = Files.exists(prodMapPath);

			// 1) In both modes we append; write headers if missing
			try (BufferedWriter csvWriter1 = Files.newBufferedWriter(xmlMapPath, StandardOpenOption.CREATE,
					StandardOpenOption.APPEND);
					CSVPrinter csvPrinter1 = new CSVPrinter(csvWriter1, CSVFormat.DEFAULT);
					BufferedWriter csvWriter2 = Files.newBufferedWriter(prodMapPath, StandardOpenOption.CREATE,
							StandardOpenOption.APPEND);
					CSVPrinter csvPrinter2 = new CSVPrinter(csvWriter2, CSVFormat.DEFAULT)) {

				if (!xmlMapExists) {
					csvPrinter1.printRecord((Object[]) XML_MAP_HEADER);
					csvPrinter1.flush();
				}
				if (!prodMapExists) {
					csvPrinter2.printRecord((Object[]) PROD_MAP_HEADER);
					csvPrinter2.flush();
				}

				ExecutorService pool = Executors.newFixedThreadPool(THREAD_COUNT);
				AtomicInteger submitted = new AtomicInteger(0);
				AtomicInteger completed = new AtomicInteger(0);

				if (!testMode) {
					// -------- normal incremental mode (original behavior) --------
					HashMap<String, String> processedKeys = loadProcessedZips(xmlMapPath);
					for (ScanTarget t : targets) {
						Logger.log("scanning: " + t.sourceType());
						try (DirectoryStream<Path> stream = Files.newDirectoryStream(t.dir(), "*.zip")) {
							for (Path zipPath : stream) {
								String zipName = zipPath.getFileName().toString();

								// Test if we have processed this file before
								if (processedKeys.containsKey(zipName)) {
									if (processedKeys.get(zipName).contentEquals(t.sourceType())) {
										continue;
									}
								}

								submitted.incrementAndGet();
								pool.submit(() -> {
									try {
										Path outDir = xmlOutputDirPath.resolve(t.sourceType());
										Files.createDirectories(outDir);
										processZipFile(zipPath, outDir, csvPrinter1, csvPrinter2, t.sourceType(), null,
												null);
									} catch (Exception ex) {
										Logger.error("Error processing " + zipPath + ": " + ex.getMessage());
										ex.printStackTrace();
									} finally {
										int done = completed.incrementAndGet();
										if (done % PROGRESS_EVERY == 0) {
											Logger.info("[ZipFileExtractor] Processed " + done + " ZIPs...");
										}
									}
								});
							}
						}
					}
				} else {
					// ----------------------- test mode ----------------------------
					// Only ZIPs listed in PRODUCT_LABEL_MAP.csv and/or SPL_ZIP_XML_MAP.csv
					Set<String> allowedZips = new HashSet<>();
					allowedZips.addAll(loadZipSetFromCsv(prodMapPath, "ZipFileName"));
					allowedZips.addAll(loadZipSetFromCsv(xmlMapPath, "ZipFileName"));

					// Existing CSV rows, so we can avoid duplicate appends:
					// - For XML map: map "sourceType::zip" -> set of xml names already recorded
					Map<String, Set<String>> existingXmlByKey = loadExistingXmlByKey(xmlMapPath);
					// - For product map: set of zips already recorded
					Set<String> existingProdZips = loadZipSetFromCsv(prodMapPath, "ZipFileName");

					Logger.info("Test mode: restricting to " + allowedZips.size() + " ZIP(s) from CSV maps.");

					for (ScanTarget t : targets) {
						try (DirectoryStream<Path> stream = Files.newDirectoryStream(t.dir(), "*.zip")) {
							for (Path zipPath : stream) {
								String zipName = zipPath.getFileName().toString();
								if (!allowedZips.contains(zipName))
									continue; // strictly restrict

								String key = t.sourceType() + "::" + zipName;
								Set<String> alreadyXmls = existingXmlByKey.getOrDefault(key,
										java.util.Collections.emptySet());

								submitted.incrementAndGet();
								pool.submit(() -> {
									try {
										Path outDir = xmlOutputDirPath.resolve(t.sourceType());
										Files.createDirectories(outDir);
										// Always (re)extract; only append missing CSV rows
										processZipFile(zipPath, outDir, csvPrinter1, csvPrinter2, t.sourceType(),
												alreadyXmls, existingProdZips);
									} catch (Exception ex) {
										Logger.error("Error processing " + zipPath + ": " + ex.getMessage());
										ex.printStackTrace();
									} finally {
										int done = completed.incrementAndGet();
										if (done % PROGRESS_EVERY == 0) {
											Logger.info("[ZipFileExtractor] Processed " + done + " ZIPs...");
										}
									}
								});
							}
						}
					}
				}

				if (submitted.get() == 0) {
					Logger.info(testMode ? "Test mode: no matching ZIPs to process (check your CSV maps)."
							: "No new ZIP files to process. CSV maps are up to date.");
				} else {
					pool.shutdown();
					pool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
					Logger.info((testMode ? "Test-mode extraction" : "Incremental extraction") + " complete ("
							+ submitted.get() + " ZIPs).");
				}

			} catch (IOException e) {
				Logger.error("Error appending to file: " + e.getMessage());
			}

		} catch (

		Exception e) {
			Logger.error("Fatal error in extractXmlFiles(" + testMode + ")");
			e.printStackTrace();
		}
	}

	public static Map<String, List<String>> getGuidXmlMap(String srcPath, Map<String, List<String>> maps,
			HashMap<String, String> guidSrcType, String SPL_ZIP_XML_MAP) {

		// This is a potential area to explore... for now, we
		// going to add all xmlFiles to the GUID if it matches
		// example: nicorette gum was prescription, then went OTC
		// therefore, later SPLs have a different source type
		// than earlier ones... you may want to keep the source type
		// check in place if you have not fully QC'd your spl data
		boolean checkSourceTypeConsistency = false;

		// Set up the CSV format with the pipe delimiter and header support
		CSVFormat csvFormat = CSVFormat.DEFAULT.withDelimiter(',')
				.withHeader("ZipFileName", "XmlFileName", "SourceType").withIgnoreHeaderCase().withTrim();

		try (FileReader reader = new FileReader(SPL_ZIP_XML_MAP);
				CSVParser csvParser = new CSVParser(reader, csvFormat)) {
			boolean firstLine = true;

			for (CSVRecord record : csvParser) {
				try {
					// Check if the record has exactly 3 fields
					if (record.size() != 3) {
						Logger.error("Invalid line in CSV: " + record.toString());
						continue;
					}

					// Convert all products to uppercase for direct comparison
					String zipFile = record.get("ZipFileName").trim();
					String sourceType = record.get("SourceType").trim();
					String xmlFile = srcPath + "xml_files/" + sourceType + "/" + record.get("XmlFileName").trim();

					if (firstLine == true && zipFile.contentEquals("ZipFileName")) {
						firstLine = false;
					} else {

						// Ensure product name, zip file and source type are not empty
						if (xmlFile.isEmpty() || zipFile.isEmpty() || sourceType.isEmpty()) {
							Logger.error("Missing data in CSV: " + record.toString());
							continue;
						} else {

							// Confirm the XML file exists
							Path xmlPath = Paths.get(xmlFile);
							if (Files.exists(xmlPath) == true) {
								if (zipFile.contains("_")) {
									String guid = zipFile.split("_")[1];
									guid = guid.replace(".zip", "");
									if (maps.containsKey(guid) == false) {
										List<String> xmlFiles = new ArrayList<>();
										xmlFiles.add(xmlFile);
										maps.put(guid, xmlFiles);
										guidSrcType.put(guid, sourceType);
									} else {
										if (guidSrcType.containsKey(guid)) {

											if (checkSourceTypeConsistency) {

												if (guidSrcType.get(guid).contentEquals(sourceType)) {
													maps.get(guid).add(xmlFile);
												} else {
													Logger.error("Skip XML entry, GUID source type conflict found: ["
															+ srcPath + "] " + guid + " Old src type: "
															+ guidSrcType.get(guid));
												}
											} else {
												// Skip the source agreement check and simply add the XML file
												maps.get(guid).add(xmlFile);
											}
										} else {
											Logger.log("Missing source type for: " + guid);
										}

									}
								}
							} else {
								Logger.error("Mapped XML file does not exist: " + xmlFile);
							}
						}
					}

				} catch (IllegalArgumentException e) {
					Logger.error("Error processing record: " + record.toString());
				}
			}

		} catch (Exception e) {
			Logger.log("Error processing SPL map file: " + e.toString());
		}

		// Logger.log("Lines processed: " + counter);
		Logger.log("Total GUID maps: " + maps.size());
		return maps;
	}

	// --- ZIP processing -------------------------------------------------------
	@SuppressWarnings("unused")
	private void processZipFile(Path zipPath, Path outputDirPath, CSVPrinter csvPrinter1, CSVPrinter csvPrinter2) {
		// Default to "prescription" — tests don't assert SourceType and
		// don't rely on a source-specific subfolder in outputDirPath.
		processZipFile(zipPath, outputDirPath, csvPrinter1, csvPrinter2, "prescription", null, null);
	}

	/**
	 * Processes a single ZIP: - extracts all *.xml (case-insensitive) under
	 * outputDirPath - appends to CSVs, unless those rows already exist (when sets
	 * are provided)
	 *
	 * @param alreadyXmlsForKey existing XmlFileName entries for this
	 *                          "sourceType::zip"
	 * @param existingProdZips  existing ZipFileName entries already recorded in
	 *                          product map
	 */
	private void processZipFile(Path zipPath, Path outputDirPath, CSVPrinter csvPrinter1, CSVPrinter csvPrinter2,
			String sourceType, Set<String> alreadyXmlsForKey, Set<String> existingProdZips) {

		final String zipBaseName = zipPath.getFileName().toString();
		final boolean haveXmlDedupe = (alreadyXmlsForKey != null);
		final boolean haveProdDedupe = (existingProdZips != null);

		try (ZipInputStream zis = new ZipInputStream(new BufferedInputStream(Files.newInputStream(zipPath)))) {
			ZipEntry entry;
			byte[] buffer = new byte[64 * 1024];

			while ((entry = zis.getNextEntry()) != null) {
				try {
					String entryName = entry.getName();
					if (entry.isDirectory())
						continue;
					if (entryName == null || !entryName.toLowerCase(Locale.ROOT).endsWith(".xml"))
						continue;

					// Normalize & defend against Zip Slip
					entryName = entryName.replace('\\', '/');
					Path target = outputDirPath.resolve(entryName).normalize();
					if (!target.startsWith(outputDirPath)) {
						Logger.error("Skipping suspicious entry (Zip Slip): " + entryName + " in " + zipBaseName);
						continue;
					}

					// Ensure subdirectories exist
					Path parent = target.getParent();
					if (parent != null)
						Files.createDirectories(parent);

					// Write the entry (overwrite is fine; this also restores missing XMLs)
					try (BufferedOutputStream bos = new BufferedOutputStream(Files.newOutputStream(target))) {
						int read;
						while ((read = zis.read(buffer)) != -1) {
							bos.write(buffer, 0, read);
						}
					}

					// CSV: Zip -> Xml
					boolean writeXmlRow = true;
					boolean fail = false;

					if (haveXmlDedupe && alreadyXmlsForKey.contains(entryName)) {
						writeXmlRow = false;
					}
					if (writeXmlRow) {

						synchronized (csvPrinter1) {

							// Mismatch types
							if (sourceType.contentEquals("prescription")) {
								boolean otc = getOtcProductIndicated(target.toString());
								if (otc) {
									fail = true;
								}
							}

							if (sourceType.contentEquals("otc")) {
								boolean otc = getOtcProductIndicated(target.toString());
								if (!otc) {
									fail = true;
								}
							}

							// Always remove bulk ingredient and animal products
							boolean bulkIngredient = getBulkIngredient(target.toString());
							if (bulkIngredient) {
								fail = true;
							}

							boolean isAnimalProduct = getNonhumanUseProductIndicated(target.toString());
							if (isAnimalProduct) {
								fail = true;
							}

							if (!fail) {
								csvPrinter1.printRecord(zipBaseName, entryName, sourceType);
							} else {
								Logger.log("Invalid archive categorization: " + zipBaseName);
							}
						}
					}

					// Product name row (usually already present in PRODUCT_LABEL_MAP.csv)
					// Only write if not already present for this zip
					boolean writeProdRow = true;
					if (haveProdDedupe && existingProdZips.contains(zipBaseName)) {
						writeProdRow = false;
					}
					if (writeProdRow && !fail) {
						String productName = getManufactureProductName(target.toString());
						if (StringUtils.isNotBlank(productName)) {
							synchronized (csvPrinter2) {
								csvPrinter2.printRecord(zipBaseName, productName);
							}
						}
					}

				} catch (Exception perEntryEx) {
					Logger.error("Error processing entry " + entry.getName() + " in " + zipBaseName + ": "
							+ perEntryEx.getMessage());
				} finally {
					zis.closeEntry();
				}
			}
		} catch (Exception e) {
			Logger.error("Error processing ZIP: " + zipBaseName + " :: " + e.toString());
			e.printStackTrace();
		}
	}

	// --- CSV state loaders ----------------------------------------------------
	/**
	 * Set of processed keys from XML map: ZipFileName,XmlFileName,SourceType
	 */
	private HashMap<String, String> loadProcessedZips(Path xmlMapPath) {
		HashMap<String, String> zips = new HashMap<>();
		if (!Files.exists(xmlMapPath))
			return zips;

		try (BufferedReader reader = Files.newBufferedReader(xmlMapPath);
				CSVParser parser = CSVFormat.DEFAULT.builder().setSkipHeaderRecord(true).setHeader().build()
						.parse(reader)) {

			for (CSVRecord r : parser) {
				String zip = r.isMapped("ZipFileName") ? r.get("ZipFileName") : null;
				if (StringUtils.isBlank(zip))
					continue;

				String xmlFile = r.isMapped("XmlFileName") ? r.get("XmlFileName") : null;
				if (StringUtils.isBlank(xmlFile))
					continue;

				String srcType = r.isMapped("SourceType") ? r.get("SourceType") : null;
				if (StringUtils.isBlank(srcType))
					continue;

				zips.put(zip, srcType);
			}
		} catch (IOException e) {
			Logger.error("Warning: could not read existing XML map; proceeding as if empty. " + e.getMessage());
		}
		return zips;
	}

	/**
	 * Reads a CSV file and returns all values from the given ZipFileName column.
	 */
	private Set<String> loadZipSetFromCsv(Path csvPath, String zipHeader) {
		Set<String> zips = new HashSet<>();
		if (!Files.exists(csvPath))
			return zips;

		try (BufferedReader reader = Files.newBufferedReader(csvPath);
				CSVParser parser = CSVFormat.DEFAULT.builder().setSkipHeaderRecord(true).setHeader().build()
						.parse(reader)) {

			if (!parser.getHeaderMap().containsKey(zipHeader))
				return zips;
			for (CSVRecord r : parser) {
				String zip = r.get(zipHeader);
				if (StringUtils.isNotBlank(zip))
					zips.add(zip);
			}
		} catch (IOException e) {
			Logger.error("Warning: could not read " + csvPath + " :: " + e.getMessage());
		}
		return zips;
	}

	/**
	 * Builds a map of existing XML rows by "sourceType::zip" -> set(XmlFileName)
	 * from SPL_ZIP_XML_MAP.csv so we can avoid duplicate appends in test mode.
	 */
	private Map<String, Set<String>> loadExistingXmlByKey(Path xmlMapPath) {
		Map<String, Set<String>> out = new HashMap<>();
		if (!Files.exists(xmlMapPath))
			return out;

		try (BufferedReader reader = Files.newBufferedReader(xmlMapPath);
				CSVParser parser = CSVFormat.DEFAULT.builder().setSkipHeaderRecord(true).setHeader().build()
						.parse(reader)) {

			Map<String, Integer> hdr = parser.getHeaderMap();
			boolean hasZip = hdr.containsKey("ZipFileName");
			boolean hasXml = hdr.containsKey("XmlFileName");
			boolean hasSrc = hdr.containsKey("SourceType");
			if (!hasZip || !hasXml || !hasSrc)
				return out;

			for (CSVRecord r : parser) {
				String zip = r.get("ZipFileName");
				String xml = r.get("XmlFileName");
				String src = r.get("SourceType");
				if (StringUtils.isAnyBlank(zip, xml, src))
					continue;
				String key = src + "::" + zip;
				out.computeIfAbsent(key, k -> new HashSet<>()).add(xml);
			}
		} catch (IOException e) {
			Logger.error("Warning: could not read existing XML map for dedupe. " + e.getMessage());
		}
		return out;
	}

	// --- XML helpers ----------------------------------------------------------
	private static String getManufactureProductName(String xmlFile) {
		try (FileInputStream xmlContentStream = new FileInputStream(xmlFile)) {
			DocumentBuilder builder = TL_DOCUMENT_BUILDER.get();
			Document document = builder.parse(xmlContentStream);
			document.getDocumentElement().normalize();
			return extractProductName(document);
		} catch (Exception e) {
			Logger.error("Error reading XML file: " + xmlFile + " :: " + e.getMessage());
		}
		return "";
	}

	private static boolean getBulkIngredient(String xmlFile) {
		try (FileInputStream xmlContentStream = new FileInputStream(xmlFile)) {
			DocumentBuilder builder = TL_DOCUMENT_BUILDER.get();
			Document document = builder.parse(xmlContentStream);
			document.getDocumentElement().normalize();
			return extractBulkIngredientDrugLable(document);
		} catch (Exception e) {
			Logger.error("Error reading XML file: " + xmlFile + " :: " + e.getMessage());
		}
		return false;
	}

	private static boolean getNonhumanUseProductIndicated(String xmlFile) {
		try (FileInputStream xmlContentStream = new FileInputStream(xmlFile)) {
			DocumentBuilder builder = TL_DOCUMENT_BUILDER.get();
			Document document = builder.parse(xmlContentStream);
			document.getDocumentElement().normalize();
			return extractNonhumanUseDrugLable(document);
		} catch (Exception e) {
			Logger.error("Error reading XML file: " + xmlFile + " :: " + e.getMessage());
		}
		return false;
	}

	private static boolean getOtcProductIndicated(String xmlFile) {
		try (FileInputStream xmlContentStream = new FileInputStream(xmlFile)) {
			DocumentBuilder builder = TL_DOCUMENT_BUILDER.get();
			Document document = builder.parse(xmlContentStream);
			document.getDocumentElement().normalize();
			return extractOtcDrugLable(document);
		} catch (Exception e) {
			Logger.error("Error reading XML file: " + xmlFile + " :: " + e.getMessage());
		}
		return false;
	}

	/**
	 * Test if the XML explicitly claims to be an OTC drug product
	 * 
	 * @param document
	 * @return
	 */
	private static boolean extractOtcDrugLable(Document document) {
		try {
			NodeList codeList = document.getElementsByTagName("code");
			if (codeList.getLength() == 0)
				return false;

			Node rootCode = codeList.item(0);
			if (rootCode.getNodeType() != Node.ELEMENT_NODE)
				return false;

			Node displayName = rootCode.getAttributes().getNamedItem("displayName");
			if (displayName.getTextContent().toLowerCase().contains("otc drug"))
				return true;

		} catch (Exception e) {
			Logger.error("Error extracting OTC code from XML: " + e.getMessage());
		}
		return false;
	}

	private static boolean extractBulkIngredientDrugLable(Document document) {
		try {
			NodeList codeList = document.getElementsByTagName("code");
			if (codeList.getLength() == 0)
				return false;

			Node rootCode = codeList.item(0);
			if (rootCode.getNodeType() != Node.ELEMENT_NODE)
				return false;

			Node displayName = rootCode.getAttributes().getNamedItem("displayName");
			if (displayName.getTextContent().toLowerCase().contains("bulk ingredient"))
				return true;

		} catch (Exception e) {
			Logger.error("Error extracting bulk ingredient code from XML: " + e.getMessage());
		}
		return false;
	}

	private static boolean extractNonhumanUseDrugLable(Document document) {
		try {
			NodeList codeList = document.getElementsByTagName("code");
			if (codeList.getLength() == 0)
				return false;

			Node rootCode = codeList.item(0);
			if (rootCode.getNodeType() != Node.ELEMENT_NODE)
				return false;

			Node displayName = rootCode.getAttributes().getNamedItem("displayName");
			if (displayName.getTextContent().toLowerCase().contains("animal drug"))
				return true;

		} catch (Exception e) {
			Logger.error("Error non-human use code from XML: " + e.getMessage());
		}
		return false;
	}

	/**
	 * Find <manufacturedProduct><name> text and return UPPERCASE normalized value.
	 */
	private static String extractProductName(Document document) {
		try {
			NodeList structuredBodyList = document.getElementsByTagName("structuredBody");
			if (structuredBodyList.getLength() == 0)
				return "";

			Node structuredBodyNode = structuredBodyList.item(0);
			if (structuredBodyNode.getNodeType() != Node.ELEMENT_NODE)
				return "";

			Element structuredBodyElement = (Element) structuredBodyNode;
			NodeList manufacturedProductList = structuredBodyElement.getElementsByTagName("manufacturedProduct");

			for (int i = 0; i < manufacturedProductList.getLength(); i++) {
				Element manufacturedProductElement = (Element) manufacturedProductList.item(i);
				NodeList nameList = manufacturedProductElement.getElementsByTagName("name");
				if (nameList.getLength() > 0) {
					Node nameNode = nameList.item(0);
					if (nameNode.getNodeType() == Node.ELEMENT_NODE) {
						String productName = nameNode.getTextContent();
						if (StringUtils.isNotBlank(productName)) {
							return productName.replaceAll("\\s+", " ").trim().toUpperCase(Locale.ROOT);
						}
					}
				}
			}
		} catch (Exception e) {
			Logger.error("Error extracting product name from XML: " + e.getMessage());
		}
		return "";
	}
}
