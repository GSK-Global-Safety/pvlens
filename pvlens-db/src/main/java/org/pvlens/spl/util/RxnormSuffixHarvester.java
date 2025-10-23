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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.pvlens.spl.conf.ConfigLoader;

/**
 * Harvest 4-letter FDA biologic suffixes from RxNorm SBD terms, validating that
 * the stem corresponds to a known IN/PIN (mono- or multi-word, e.g. "insulin aspart").
 */
public final class RxnormSuffixHarvester {

    private RxnormSuffixHarvester() {}

    // e.g., "nivolumab-nvhy", "hyaluronidase-nvhy", "insulin aspart-xjhz"
    private static final Pattern SUFFIX_TAIL = Pattern.compile("(?i)(?<stem>[A-Za-z][A-Za-z\\s]{0,60})-([a-z]{4})\\b");

    /** Result bundle: suffix -> base name, with one example term for review. */
    public static final class HarvestResult {
        public final Map<String,String> suffixToBase = new LinkedHashMap<>();     // "rmbw" -> "relatlimab"
        public final Map<String,String> suffixToExample = new LinkedHashMap<>();  // "rmbw" -> "nivolumab-rmbw 240 mg ..."
        public LocalDate asOf = LocalDate.now();
    }

    public static HarvestResult harvest(ConfigLoader cfg) {
        HarvestResult out = new HarvestResult();

        // 1) Build IN/PIN lexicon for exact matching (mono and multi-word)
        Set<String> inpinLex = new HashSet<>();
        try (Connection c = open(cfg)) {
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT TERM FROM pvlens.RXNORM WHERE TTY IN ('IN','PIN')")) {
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String term = safeLower(rs.getString(1));
                        if (!term.isBlank()) inpinLex.add(term);
                    }
                }
            }

            // 2) Walk SBD terms and extract candidate suffixes
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT TERM FROM pvlens.RXNORM WHERE TTY = 'SBD'")) {
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String term = safeLower(rs.getString(1)); // e.g., "... nivolumab-nvhy 120 mg/ml ..."
                        if (term.isBlank()) continue;
                        extractFromSbdTerm(term, inpinLex, out);
                    }
                }
            }

        } catch (Exception e) {
            Logger.error("RxnormSuffixHarvester: " + e.getMessage());
        }

        Logger.info("RxnormSuffixHarvester: harvested " + out.suffixToBase.size() + " suffixes from SBD.");
        return out;
    }

    private static void extractFromSbdTerm(String term, Set<String> inpinLex, HarvestResult out) {
        // We'll scan words with a sliding window to recover multi-word stems.
        // Tokenize (letters & spaces only) so we can form "insulin aspart".
        // Keep the original term for example provenance.
        Matcher m = SUFFIX_TAIL.matcher(term);
        while (m.find()) {
            // m.group(0) covers "...-xxxx", group(1) is the stem-ish chunk (may be multi-word or a trailing word)
            String rawStemChunk = m.group("stem").trim();       // may be "nivolumab", or "insulin aspart"
            String sfx = m.group(2).toLowerCase(Locale.ROOT);

            String base = resolveBaseName(rawStemChunk, inpinLex);
            if (base == null) continue; // not a real IN/PIN base, skip

            out.suffixToBase.putIfAbsent(sfx, base);
            out.suffixToExample.putIfAbsent(sfx, term);
        }
    }

    /**
     * Try to resolve a base IN/PIN from the chunk before "-xxxx".
     * Strategy:
     *  - First try the whole chunk (e.g., "insulin aspart").
     *  - If not found, try the last 1..3 tokens concatenated with spaces, using preceding tokens in the term.
     *    (e.g., if chunk is "aspart", we look back one token to form "insulin aspart".)
     */
    private static String resolveBaseName(String rawStemChunk, Set<String> inpinLex) {
        String chunk = rawStemChunk.replaceAll("[^a-z\\s]", " ").replaceAll("\\s+", " ").trim();
        if (chunk.isEmpty()) return null;

        // 1) Exact match on the chunk as-is
        if (inpinLex.contains(chunk)) return chunk;

        // 2) If the chunk is multiple words, try its suffixes (last 2, last 1)
        String[] words = chunk.split(" ");
        // Try last 2 words
        if (words.length >= 2) {
            String two = words[words.length-2] + " " + words[words.length-1];
            if (inpinLex.contains(two)) return two;
        }
        // Try last 1 word (already tried above via exact chunk if chunk was one word)
        if (words.length >= 1) {
            String one = words[words.length-1];
            if (inpinLex.contains(one)) return one;
        }

        // If we didn't find a match, give up for this occurrence.
        return null;
    }

    private static Connection open(ConfigLoader cfg) throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        String url = String.format("jdbc:mysql://%s:%s/%s?useSSL=false", cfg.getDbHost(), cfg.getDbPort(), cfg.getDbName());
        return DriverManager.getConnection(url, cfg.getDbUser(), cfg.getDbPass());
    }

    private static String safeLower(String s) {
        return (s == null) ? "" : s.toLowerCase(Locale.ROOT).trim();
    }
}
