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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.pvlens.spl.util.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Extracts NDC codes from SPL XML files using a two-stage strategy:
 * <ol>
 *   <li>Fast regex sweep over raw bytes (robust even if XML is malformed).</li>
 *   <li>Authoritative DOM parse for &lt;code codeSystem="2.16.840.1.113883.6.69" code="..."/&gt;.</li>
 * </ol>
 * <p>Returns unique codes while preserving first-seen order.</p>
 */
public class NdcExtractor {

	/** Secure, reusable DOM builder per thread (XXE/DTD disabled). */
	private static final ThreadLocal<DocumentBuilder> DB = ThreadLocal.withInitial(() -> {
		try {
			DocumentBuilderFactory f = DocumentBuilderFactory.newInstance();
			f.setNamespaceAware(true);
			f.setValidating(false);
			f.setExpandEntityReferences(false);
			f.setXIncludeAware(false);

			// Security hardening
			f.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
			f.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
			f.setFeature("http://xml.org/sax/features/external-general-entities", false);
			f.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
			f.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
			try {
				f.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
				f.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
			} catch (Throwable ignore) { /* not all impls support attributes */ }

			return f.newDocumentBuilder();
		} catch (Exception e) {
			throw new RuntimeException("Failed to init XML builder", e);
		}
	});

	/** Common hyphenated NDC pattern (e.g., 69367-314-01, 12345-6789-0). */
	private static final Pattern NDC_FALLBACK = Pattern.compile("\\b\\d{4,5}-\\d{3,4}-\\d{1,2}\\b");

	public NdcExtractor() { }

	// -------- raw helpers --------

	/** Extract hyphenated NDCs from arbitrary text via regex. */
	private static List<String> extractByRegex(CharSequence txt) {
		if (txt == null) return Collections.emptyList();
		LinkedHashSet<String> out = new LinkedHashSet<>();
		Matcher m = NDC_FALLBACK.matcher(txt);
		while (m.find()) out.add(m.group());
		return new ArrayList<>(out);
	}

	/** Read file contents into bytes (tiny SPLs make this cheap). */
	private static byte[] readAll(Path p) throws IOException {
		return java.nio.file.Files.readAllBytes(p);
	}

	/** Decode UTF-8 and remove BOM, if present. */
	private static String bytesToUtf8Text(byte[] bytes) {
		if (bytes == null || bytes.length == 0) return "";
		String s = new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
		// Strip UTF-8 BOM
		if (!s.isEmpty() && s.charAt(0) == '\uFEFF') s = s.substring(1);
		return s;
	}

	/**
	 * Extract a de-duplicated, in-order list of NDC codes from the given SPL XML file path.
	 * <p>Never throws; returns {@code Collections.emptyList()} on errors.</p>
	 */
	public List<String> getNdcCodes(String xmlFile) {
		File f = new File(xmlFile);
		if (!f.exists()) {
			Logger.error("XML file not found: " + xmlFile);
			return Collections.emptyList();
		}

		final Path path = f.toPath();
		List<String> ndcCodes = new ArrayList<>();

		try {
			// Read once so we can: (1) fast regex sweep, (2) parse DOM if needed, (3) handle BOM/junk
			byte[] bytes = readAll(path);

			// (0) Very fast regex sweep first (works even if XML is malformed)
			String rawText = bytesToUtf8Text(bytes);
			List<String> regexHits = extractByRegex(rawText);
			if (!regexHits.isEmpty()) {
				ndcCodes.addAll(regexHits); // provisional results
			}

			// (1) Try DOM parse for authoritative codeSystem hits.
			// If it fails (e.g., "Content is not allowed in prolog."), keep regex results.
			try (ByteArrayInputStream in = new ByteArrayInputStream(bytes)) {
				DocumentBuilder builder = DB.get();
				Document document = builder.parse(in);
				document.getDocumentElement().normalize();

				// 1a) <code codeSystem="2.16.840.1.113883.6.69" code="...">
				NodeList codeNodes = document.getElementsByTagName("code");
				for (int i = 0; i < codeNodes.getLength(); i++) {
					Node n = codeNodes.item(i);
					if (n.getNodeType() != Node.ELEMENT_NODE) continue;
					Element el = (Element) n;
					String codeSystem = el.getAttribute("codeSystem");
					String codeValue  = el.getAttribute("code");
					if ("2.16.840.1.113883.6.69".equals(codeSystem) && codeValue != null && !codeValue.isBlank()) {
						ndcCodes.add(codeValue.trim());
					}
				}

				// 1b) manufacturedProduct/code path (fallback within DOM)
				if (ndcCodes.isEmpty()) {
					NodeList manufactured = document.getElementsByTagName("manufacturedProduct");
					for (int i = 0; i < manufactured.getLength(); i++) {
						Node mp = manufactured.item(i);
						if (mp.getNodeType() != Node.ELEMENT_NODE) continue;
						NodeList children = mp.getChildNodes();
						for (int j = 0; j < children.getLength(); j++) {
							Node child = children.item(j);
							if (child.getNodeType() == Node.ELEMENT_NODE && "code".equals(child.getNodeName())) {
								Element c = (Element) child;
								String codeSystem = c.getAttribute("codeSystem");
								String codeValue  = c.getAttribute("code");
								if ("2.16.840.1.113883.6.69".equals(codeSystem)
										&& codeValue != null && !codeValue.isBlank()) {
									ndcCodes.add(codeValue.trim());
								}
							}
						}
					}
				}

				// 1c) Last DOM-based fallback: mine textContent if still empty (keeps order)
				if (ndcCodes.isEmpty()) {
					String allText = document.getDocumentElement().getTextContent();
					ndcCodes.addAll(extractByRegex(allText));
				}

			} catch (org.xml.sax.SAXParseException sax) {
				// Malformed XML (e.g., "Content is not allowed in prolog.")
				// We already did a regex sweep; warn & continue.
				Logger.warn("Malformed SPL XML (skipping DOM parse): " + xmlFile + " — " + sax.getMessage());
			} catch (Exception domEx) {
				// Any other DOM issue: keep regex results and continue
				Logger.warn("XML parse issue for '" + xmlFile + "': " + domEx.getMessage());
			}

			// Dedup while preserving order (regex + DOM contributions)
			if (!ndcCodes.isEmpty()) {
				LinkedHashSet<String> dedup = new LinkedHashSet<>(ndcCodes);
				ndcCodes.clear();
				ndcCodes.addAll(dedup);
			}

		} catch (Exception outer) {
			// Hard guard: no exception escapes; processing continues
			Logger.error("Error extracting NDC codes from '" + xmlFile + "': " + outer.getMessage(), outer);
			return Collections.emptyList();
		}

		return ndcCodes;
	}

	/**
	 * Previous implementation kept for reference/regression comparisons.
	 * Prefer {@link #getNdcCodes(String)}. Always returns a non-null list.
	 */
	public List<String> old_getNdcCodes(String xmlFile) {
		File xmlInputFile = new File(xmlFile);
		if (!xmlInputFile.exists()) {
			Logger.error("XML file not found: " + xmlFile);
			return Collections.emptyList();
		}

		List<String> ndcCodes = new ArrayList<>();
		try (FileInputStream xmlContentStream = new FileInputStream(xmlFile)) {

			DocumentBuilder builder = DB.get();
			Document document = builder.parse(xmlContentStream);
			document.getDocumentElement().normalize();

			// 1) Most reliable signal: code elements that use the NDC code system
			NodeList codeNodes = document.getElementsByTagName("code");
			for (int i = 0; i < codeNodes.getLength(); i++) {
				Node n = codeNodes.item(i);
				if (n.getNodeType() != Node.ELEMENT_NODE) continue;
				Element el = (Element) n;
				String codeSystem = el.getAttribute("codeSystem");
				String codeValue  = el.getAttribute("code");
				if ("2.16.840.1.113883.6.69".equals(codeSystem) && codeValue != null && !codeValue.isBlank()) {
					ndcCodes.add(codeValue.trim());
				}
			}

			// 2) manufacturedProduct path
			if (ndcCodes.isEmpty()) {
				NodeList manufactured = document.getElementsByTagName("manufacturedProduct");
				for (int i = 0; i < manufactured.getLength(); i++) {
					Node mp = manufactured.item(i);
					if (mp.getNodeType() != Node.ELEMENT_NODE) continue;
					NodeList children = mp.getChildNodes();
					for (int j = 0; j < children.getLength(); j++) {
						Node child = children.item(j);
						if (child.getNodeType() == Node.ELEMENT_NODE && "code".equals(child.getNodeName())) {
							Element c = (Element) child;
							String codeSystem = c.getAttribute("codeSystem");
							String codeValue  = c.getAttribute("code");
							if ("2.16.840.1.113883.6.69".equals(codeSystem) && codeValue != null && !codeValue.isBlank()) {
								ndcCodes.add(codeValue.trim());
							}
						}
					}
				}
			}

			// 3) As fallback, mine text for hyphenated NDC patterns
			if (ndcCodes.isEmpty()) {
				String allText = document.getDocumentElement().getTextContent();
				ndcCodes.addAll(extractByRegex(allText));
			}

			// Deduplicate while preserving order
			LinkedHashSet<String> dedup = new LinkedHashSet<>(ndcCodes);
			ndcCodes.clear();
			ndcCodes.addAll(dedup);

		} catch (Exception e) {
			Logger.error("Error extracting NDC codes from '" + xmlFile + "': " + e.getMessage(), e);
		}
		return ndcCodes;
	}
}
