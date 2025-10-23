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


import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

import org.junit.jupiter.api.*;

class StopwordRemoverTest {

	private StopwordRemover remover;

	private static List<String> singleTokens;
	private static List<List<String>> multiPhrases;

	@BeforeAll
	static void loadStopwordsFile() throws Exception {
		Path p = Path.of("src/main/resources/stopwords.txt");
		assertTrue(Files.exists(p), "stopwords.txt must exist at " + p.toAbsolutePath());

		List<String> lines = Files.readAllLines(p, StandardCharsets.UTF_8);
		List<List<String>> phrases = lines.stream().map(String::trim).filter(s -> !s.isEmpty() && !s.startsWith("#"))
				.map(s -> Arrays.asList(s.toLowerCase().split("\\s+"))).collect(Collectors.toList());

		singleTokens = phrases.stream().filter(lst -> lst.size() == 1).map(lst -> lst.get(0))
				.collect(Collectors.toList());

		multiPhrases = phrases.stream().filter(lst -> lst.size() > 1).collect(Collectors.toList());

		// We want at least some data to run meaningful tests
		assertFalse(phrases.isEmpty(), "stopwords.txt should contain at least one entry");
	}

	@BeforeEach
	void setup() {
		remover = StopwordRemover.getInstance();
		// warm up the lazy loader (no-op for content)
		remover.removeStopwords("warm up");
	}

	@Test
	void nullAndBlankInput() {
		assertEquals("", remover.removeStopwords(null));
		assertEquals("", remover.removeStopwords("   "));
	}

	@Test
	void singleWordStopIsRecognized() {
		// If there are no single-word stopwords, skip
		Assumptions.assumeTrue(!singleTokens.isEmpty(), "No single-word stopwords found; skipping test.");
		String token = singleTokens.get(0);
		assertTrue(remover.isStopword(token), "Expected single-word token from file to be recognized as stopword");
	}

	@Test
	void multiWordPhraseStop() {
		Assumptions.assumeTrue(!multiPhrases.isEmpty(), "No multi-word stop phrases found; skipping test.");
		String phrase = String.join(" ", multiPhrases.get(0));
		assertTrue(remover.isStopword(phrase), "Expected multi-word phrase from file to be recognized as stop phrase");
	}

	@Test
	void keepsNonStopTokens() {
		// Choose unlikely tokens to be in your list
		String text = "xylophone zebraquark";
		String cleaned = remover.removeStopwords(text);
		assertEquals("xylophone zebraquark", cleaned);
	}

	// ... imports and class header unchanged ...

	@Test
	void greedyLongestPhraseWins() {
		// Find a multi-word phrase whose first token is also a single-word stopword.
		Optional<List<String>> candidate = multiPhrases.stream().filter(lst -> singleTokens.contains(lst.get(0)))
				.findFirst();

		Assumptions.assumeTrue(candidate.isPresent(),
				"No multi-word phrase whose first token is also a single stopword; skipping test.");

		// Use tail tokens that are almost certainly not stopwords
		// (avoid “this”, “that”, etc. because your list may include them).
		String tail = "xylophonium example";

		String phrase = String.join(" ", candidate.get());
		String cleaned = remover.removeStopwords(phrase + " " + tail);

		// Assert: the whole candidate phrase is removed, and the tail remains intact.
		// We don't assert exact equality of whole string in case upstream normalization
		// trims punctuation/whitespace differently; just check the important parts.
		for (String tok : candidate.get()) {
			assertFalse(Arrays.asList(cleaned.split("\\s+")).contains(tok),
					"Phrase token should have been removed: " + tok);
		}
		assertTrue(cleaned.endsWith(tail), "Expected tail to remain: " + tail + " but got: " + cleaned);
	}


	@Test
	void punctuationIsHandled() {
	    Assumptions.assumeTrue(!multiPhrases.isEmpty(), "No multi-word phrases; skipping punctuation test.");

	    List<String> phraseTokens = multiPhrases.get(0);
	    String phrase = String.join(" ", phraseTokens);

	    // Deterministic tail tokens that should not appear in your stopword list
	    List<String> tailTokens = Arrays.asList("xylophonium", "blortimus", "qruxel");

	    // Sanity: if any tail token is (unexpectedly) a stopword phrase, skip to avoid false negatives.
	    for (String t : tailTokens) {
	        Assumptions.assumeFalse(remover.isStopword(t), "Tail token unexpectedly in stopword list: " + t);
	    }

	    // Phrase immediately followed by punctuation to exercise the boundary condition
	    String input = phrase + ", " + String.join(" ", tailTokens) + ".";

	    String cleaned = remover.removeStopwords(input);
	    List<String> outTokens = Arrays.asList(cleaned.split("\\s+"));

	    // The phrase tokens must be gone
	    for (String tok : phraseTokens) {
	        assertFalse(outTokens.contains(tok), "Phrase token should have been removed near punctuation: " + tok);
	    }

	    // The tail tokens must remain
	    for (String t : tailTokens) {
	        assertTrue(outTokens.contains(t), "Expected tail token to remain: " + t + " (cleaned: " + cleaned + ")");
	    }

	    assertFalse(cleaned.isBlank(), "Output should not be blank after removing the phrase.");
	}

}
