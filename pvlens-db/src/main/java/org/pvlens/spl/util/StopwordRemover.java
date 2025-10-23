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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.InputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.pvlens.spl.umls.UmlsLoader;

/**
 * Singleton class for removing stopwords from text.
 * Supports both single-word and multi-word stop phrases.
 */
public class StopwordRemover {

	// Singleton instance
	private static volatile StopwordRemover instance;

	// Set of stop phrases, each phrase represented as a List of tokens
	private final Set<List<String>> stopwords = new HashSet<>();

	// Length of the longest multi-word stop phrase (in tokens)
	private volatile int maxPhraseLen = 1;

	// Default stopword resource path (classpath), with FS fallback
	private static final String CLASSPATH_STOPWORDS = "stopwords.txt";
	private static final String FS_STOPWORDS = "src/main/resources/stopwords.txt";

	private StopwordRemover() { /* lazy load */ }

	/** Get singleton instance. */
	public static StopwordRemover getInstance() {
		if (instance == null) {
			synchronized (StopwordRemover.class) {
				if (instance == null) {
					instance = new StopwordRemover();
				}
			}
		}
		return instance;
	}

	/** Ensure the stopword list is loaded exactly once (thread-safe). */
	private void ensureStopwordsLoaded() {
		if (stopwords.isEmpty()) {
			synchronized (stopwords) {
				if (stopwords.isEmpty()) {
					loadStopwords();
				}
			}
		}
	}

	/**
	 * Load stopwords from classpath resource {@code stopwords.txt};
	 * if missing, fall back to {@code src/main/resources/stopwords.txt}.
	 * Lines starting with '#' or blank lines are ignored. Each line may
	 * contain a single token or a multi-word phrase.
	 */
	private void loadStopwords() {
		// Include single-letter tokens a..z as stopwords
		for (char ch = 'a'; ch <= 'z'; ch++) {
			stopwords.add(Collections.singletonList(String.valueOf(ch)));
		}

		// Try classpath first
		boolean loaded = false;
		try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(CLASSPATH_STOPWORDS)) {
			if (in != null) {
				try (BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
					readStopFileInto(br);
					loaded = true;
				}
			}
		} catch (IOException ioe) {
			Logger.warn("Stopwords: classpath read failed: {}", ioe.getMessage());
		}

		// Fallback to filesystem (dev/test mode)
		if (!loaded) {
			try (BufferedReader br = new BufferedReader(new FileReader(FS_STOPWORDS, StandardCharsets.UTF_8))) {
				readStopFileInto(br);
				loaded = true;
			} catch (IOException e) {
				Logger.error("Error loading stopwords: {}", e.getMessage());
			}
		}
	}

	private void readStopFileInto(BufferedReader reader) throws IOException {
		String line;
		while ((line = reader.readLine()) != null) {
			String trimmed = line.trim();
			if (trimmed.isEmpty() || trimmed.startsWith("#")) continue;

			List<String> phrase = Arrays.asList(trimmed.toLowerCase().split("\\s+"));
			stopwords.add(phrase);
			if (phrase.size() > maxPhraseLen) {
				maxPhraseLen = phrase.size();
			}
		}
	}

	/**
	 * Returns {@code true} if the given text (single or multi-word)
	 * is exactly a stop phrase. Null or blank → false.
	 */
	public boolean isStopword(String text) {
		if (text == null || text.isBlank()) return false;
		ensureStopwordsLoaded();
		List<String> tokens = Arrays.asList(text.trim().toLowerCase().split("\\s+"));
		return stopwords.contains(tokens);
	}

	/**
	 * Remove stopwords (single- or multi-word phrases) from input text.
	 * Null or blank input → {@code ""}.
	 */
	public String removeStopwords(String text) {
		if (text == null || text.isBlank()) return "";
		ensureStopwordsLoaded();

		// Normalize first (same as before)
		String normalized = UmlsLoader.umlsCleanText(text);
		if (normalized.isBlank()) return "";

		List<String> tokens = new ArrayList<>(Arrays.asList(normalized.split("\\s+")));

		// Sliding window, greedy from longest phrase down
		for (int n = maxPhraseLen; n >= 1; n--) {
			if (tokens.size() < n) continue;
			for (int i = 0; i <= tokens.size() - n; i++) {
				List<String> window = tokens.subList(i, i + n);
				if (stopwords.contains(window)) {
					Collections.fill(window, ""); // mark for deletion
				}
			}
		}

		StringBuilder out = new StringBuilder();
		for (String tok : tokens) {
			if (tok != null && !tok.isEmpty()) {
				out.append(tok).append(' ');
			}
		}
		return out.toString().trim();
	}
}
