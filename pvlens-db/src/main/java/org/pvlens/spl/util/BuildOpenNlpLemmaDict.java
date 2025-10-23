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


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import org.pvlens.spl.conf.ConfigLoader;

import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.tokenize.SimpleTokenizer;
import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;

public class BuildOpenNlpLemmaDict {

    // ---------- Config / flags ----------
    private static final String SENT_MODEL_PATH   = "models/en-sent.bin";
    private static final String TOKEN_MODEL_PATH  = "models/en-token.bin";
    private static final String POS_MODEL_PATH    = "models/en-pos-maxent.bin";
    
    // Need full output path
    private static final String OUTPUT_DICT_PATH  = "src/main/resources/models/en-lemmatizer.dict";

    // Sampling / filtering
    private static final int MAX_ROWS_PER_TABLE   = 70_000;
    private static final boolean DEDUP            = true;
    private static final int MIN_TOKEN_LEN        = 2;
    private static final int MAX_TOKEN_LEN        = 60;

    // Optional date filter (YYYY-MM-DD). Empty => all rows.
    private static final String DATE_SINCE        = "";

    // ---------- DB (via your ConfigLoader) ----------
    private static String DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME;

    // ---------- NLP ----------
    private static SentenceDetectorME SENT_DET; // optional
    private static Tokenizer TOKENIZER;         // optional
    private static POSTaggerME POS;             // required

    // Irregular lemma maps (lowercased token -> lemma)
    private static final Map<String, String> IRREG_NOUN = new HashMap<>();
    private static final Map<String, String> IRREG_VERB = new HashMap<>();
    private static final Map<String, String> IRREG_ADJ  = new HashMap<>();
    static {
        // Nouns
        IRREG_NOUN.put("children","child"); IRREG_NOUN.put("teeth","tooth"); IRREG_NOUN.put("feet","foot");
        IRREG_NOUN.put("men","man"); IRREG_NOUN.put("women","woman"); IRREG_NOUN.put("mice","mouse");
        IRREG_NOUN.put("geese","goose"); IRREG_NOUN.put("data","datum"); IRREG_NOUN.put("indices","index");
        IRREG_NOUN.put("oxen","ox"); IRREG_NOUN.put("ves","f"); // fallback pattern handled in rules
        // Verbs
        IRREG_VERB.put("was","be"); IRREG_VERB.put("were","be"); IRREG_VERB.put("been","be"); IRREG_VERB.put("am","be"); IRREG_VERB.put("is","be"); IRREG_VERB.put("are","be");
        IRREG_VERB.put("has","have"); IRREG_VERB.put("had","have"); IRREG_VERB.put("did","do"); IRREG_VERB.put("done","do");
        IRREG_VERB.put("went","go"); IRREG_VERB.put("gone","go"); IRREG_VERB.put("saw","see"); IRREG_VERB.put("seen","see");
        IRREG_VERB.put("took","take"); IRREG_VERB.put("taken","take"); IRREG_VERB.put("made","make");
        IRREG_VERB.put("said","say"); IRREG_VERB.put("got","get"); IRREG_VERB.put("knives","knife"); // noun-ish safety
        // Adjectives
        IRREG_ADJ.put("better","good"); IRREG_ADJ.put("best","good");
        IRREG_ADJ.put("worse","bad");   IRREG_ADJ.put("worst","bad");
        IRREG_ADJ.put("farther","far"); IRREG_ADJ.put("farthest","far");
        IRREG_ADJ.put("further","far"); IRREG_ADJ.put("furthest","far");
    }

    private static final Pattern ALPHA = Pattern.compile("^[\\p{L}][\\p{L}\\p{Mn}\\p{Pd}']*$"); // words with letters/hyphen/'

    public static void main(String[] args) throws Exception {
        // Load DB config
        ConfigLoader cfg = new ConfigLoader();
        DB_HOST = cfg.getDbHost();
        DB_PORT = cfg.getDbPort();
        DB_USER = cfg.getDbUser();
        DB_PASS = cfg.getDbPass();
        DB_NAME = cfg.getDbName();

        // Init NLP
        SENT_DET  = loadSentenceModel(SENT_MODEL_PATH);
        TOKENIZER = loadTokenizer(TOKEN_MODEL_PATH); // may be SimpleTokenizer
        POS       = loadPosModel(POS_MODEL_PATH);
        if (POS == null) {
            System.err.println("ERROR: POS model failed to load. Cannot build lemmatizer dict.");
            System.exit(2);
        }

        System.out.printf("Lemma dict build start. sentModel=%s, tokModel=%s, posModel=%s%n",
                (SENT_DET != null), (TOKENIZER != null), (POS != null));

        Instant t0 = Instant.now();

        // Collect entries (token, tag, lemma) => write out
        // Use LinkedHashSet for stable order
        Set<String> triples = new LinkedHashSet<>(1 << 20);

        try (Connection conn = connect()) {
            harvestTable(conn, "SPL_IND_TEXT", "SPL_TEXT", triples);
            harvestTable(conn, "SPL_AE_TEXT",  "SPL_TEXT", triples);
            harvestTable(conn, "SPL_BOX_TEXT", "SPL_TEXT", triples);
        }

        int count = triples.size();
        File out = new File(OUTPUT_DICT_PATH);
        out.getParentFile().mkdirs();
        try (BufferedWriter w = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(out), StandardCharsets.UTF_8))) {
            for (String line : triples) w.write(line + "\n");
        }

        Instant t1 = Instant.now();
        System.out.printf("Wrote %d entries to %s (%.1fs)%n", count, OUTPUT_DICT_PATH, Duration.between(t0, t1).toMillis()/1000.0);
    }

    // ------------------ Harvesting ------------------

    private static void harvestTable(Connection conn, String table, String textCol, Set<String> out) throws SQLException {
        String dateFilter = (DATE_SINCE == null || DATE_SINCE.isBlank()) ? "" : " AND LABEL_DATE >= ?";
        String sql = "SELECT " + textCol + " FROM " + table + " WHERE 1=1 " + dateFilter + " ORDER BY ID";
        if (MAX_ROWS_PER_TABLE < Integer.MAX_VALUE) {
            sql += " LIMIT " + MAX_ROWS_PER_TABLE;
        }

        try (PreparedStatement ps = conn.prepareStatement(sql,
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            ps.setFetchSize(Integer.MIN_VALUE); // MySQL streaming
            if (!dateFilter.isBlank()) ps.setString(1, DATE_SINCE);

            System.out.printf("Streaming %s ...%n", table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String text = rs.getString(1);
                    if (text == null || text.isBlank()) continue;

                    for (String sent : detectSentences(text)) {
                        String[] toks = tokenize(sent);
                        if (toks.length == 0) continue;

                        String[] tags = POS.tag(toks);

                        for (int i = 0; i < toks.length; i++) {
                            String tok = toks[i];
                            if (tok == null) continue;
                            String tokenNorm = normalizeToken(tok);
                            if (tokenNorm.length() < MIN_TOKEN_LEN || tokenNorm.length() > MAX_TOKEN_LEN) continue;
                            if (!ALPHA.matcher(tokenNorm).matches()) continue;

                            String tag = tags[i];
                            String lemma = lemmatize(tokenNorm, tag);

                            // Write every mapping, including identity (token==lemma).
                            // DictionaryLemmatizer prefers dictionary over backoff.
                            String line = tokenNorm + "\t" + tag + "\t" + lemma;
                            if (DEDUP) out.add(line); else out.add(line); // set already dedups
                        }
                    }
                }
            }
        }
    }

    // ------------------ Lemmatization rules ------------------

    private static String lemmatize(String tokenLower, String posTag) {
        String t = tokenLower;

        // Handle punctuation-stripped empty
        if (t.isEmpty()) return t;

        // Irregulars first by POS type
        char c0 = posTag.isEmpty() ? 'X' : posTag.charAt(0);
        switch (c0) {
            case 'N': {
                String ir = IRREG_NOUN.get(t);
                if (ir != null) return ir;
                // NNS, NNPS -> NN heuristics
                if (posTag.startsWith("N") && (posTag.equals("NNS") || posTag.equals("NNPS"))) {
                    // knives -> knife / lives -> life
                    if (t.endsWith("ves") && t.length() > 3) {
                        String base = t.substring(0, t.length() - 3);
                        if (base.endsWith("i")) return base.substring(0, base.length() - 1) + "y"; // "lives"->"life" handled below
                        return base + "f"; // knives->knif(e) adjustment below
                    }
                    if (t.endsWith("ies") && t.length() > 3) return t.substring(0, t.length() - 3) + "y"; // "bodies"->"body"
                    if (t.endsWith("es") && t.length() > 2) {
                        // boxes->box, classes->class
                        return t.substring(0, t.length() - 2);
                    }
                    if (t.endsWith("s") && t.length() > 1) {
                        return t.substring(0, t.length() - 1);
                    }
                }
                return t;
            }
            case 'V': {
                String ir = IRREG_VERB.get(t);
                if (ir != null) return ir;
                if (posTag.equals("VBG")) {
                    if (t.endsWith("ing") && t.length() > 4) {
                        String base = t.substring(0, t.length() - 3);
                        // doubling: running->run, stopping->stop
                        if (base.endsWith(base.substring(base.length()-1) + "")) {
                            base = base.substring(0, base.length()-1);
                        }
                        // dropping: making->make
                        if (base.endsWith("k") && t.endsWith("king")) { /* keep */ }
                        else if (base.endsWith("e")) { /* keep */ }
                        else if (t.matches(".*[^aeiou]ing$")) { /* keep base */ }
                        return base;
                    }
                }
                if (posTag.equals("VBD") || posTag.equals("VBN")) {
                    if (t.endsWith("ied") && t.length() > 3) return t.substring(0, t.length() - 3) + "y";
                    if (t.endsWith("ed") && t.length() > 2) {
                        String base = t.substring(0, t.length() - 2);
                        // doubled consonant: stopped->stop
                        if (base.length() >= 2 && base.charAt(base.length()-1) == base.charAt(base.length()-2)) {
                            base = base.substring(0, base.length()-1);
                        }
                        // hoped->hope (keep final 'e')
                        if (!base.endsWith("e")) {
                            // naive: do nothing; many verbs don't add 'e'
                        }
                        return base;
                    }
                }
                if (posTag.equals("VBZ")) {
                    if (t.endsWith("ies") && t.length() > 3) return t.substring(0, t.length() - 3) + "y";
                    if (t.endsWith("es") && t.length() > 2) return t.substring(0, t.length() - 2);
                    if (t.endsWith("s") && t.length() > 1) return t.substring(0, t.length() - 1);
                }
                return t;
            }
            case 'J': {
                String ir = IRREG_ADJ.get(t);
                if (ir != null) return ir;
                if (posTag.equals("JJR") || posTag.equals("JJS")) {
                    if (t.endsWith("ier")) return t.substring(0, t.length()-3) + "y";
                    if (t.endsWith("iest")) return t.substring(0, t.length()-4) + "y";
                    if (t.endsWith("er")) return t.substring(0, t.length()-2);
                    if (t.endsWith("est")) return t.substring(0, t.length()-3);
                }
                return t;
            }
            case 'R': { // adverbs (RBR/RBS)
                if (posTag.equals("RBR")) return t.endsWith("er") ? t.substring(0, t.length()-2) : t;
                if (posTag.equals("RBS")) return t.endsWith("est") ? t.substring(0, t.length()-3) : t;
                return t;
            }
            default:
                return t;
        }
    }

    // ------------------ NLP helpers ------------------

    private static SentenceDetectorME loadSentenceModel(String path) {
        try (InputStream in = tryOpen(path)) {
            if (in == null) return null;
            return new SentenceDetectorME(new SentenceModel(in));
        } catch (Exception e) {
            return null;
        }
    }

    private static Tokenizer loadTokenizer(String path) {
        try (InputStream in = tryOpen(path)) {
            if (in == null) return SimpleTokenizer.INSTANCE; // fallback
            return new TokenizerME(new TokenizerModel(in));
        } catch (Exception e) {
            return SimpleTokenizer.INSTANCE;
        }
    }

    private static POSTaggerME loadPosModel(String path) {
        try (InputStream in = tryOpen(path)) {
            if (in == null) return null;
            return new POSTaggerME(new POSModel(in));
        } catch (Exception e) {
            return null;
        }
    }

    private static String[] detectSentences(String text) {
        if (SENT_DET != null) {
            try { return SENT_DET.sentDetect(text); } catch (Exception ignore) {}
        }
        return text.split("(?<=[.!?])\\s+");
    }

    private static String[] tokenize(String sentence) {
        if (TOKENIZER != null) {
            try { return TOKENIZER.tokenize(sentence); } catch (Exception ignore) {}
        }
        return sentence.split("\\s+");
    }

    // Lowercase + light punctuation folding similar to runtime normalization
    private static String normalizeToken(String s) {
        String out = s.toLowerCase(Locale.ROOT).trim();
        if (out.isEmpty()) return out;
        out = out.replace('\u2019', '\'').replace('\u2018', '\'')
                 .replace('\u2013', '-').replace('\u2014', '-').replace('\u2212', '-');
        out = out.replaceAll("\\s*'\\s*s\\b", "'s");
        out = out.replaceAll("(?<=\\p{L})-(?=\\p{L})", "-"); // keep hyphen as char
        return out;
    }

    private static InputStream tryOpen(String path) {
        // FS first
        try {
            java.nio.file.Path p = java.nio.file.Paths.get(path);
            if (java.nio.file.Files.exists(p)) return new FileInputStream(p.toFile());
        } catch (Exception ignore) {}
        // CLASSPATH
        try {
            InputStream in = BuildOpenNlpLemmaDict.class.getClassLoader().getResourceAsStream(path);
            if (in != null) return in;
        } catch (Exception ignore) {}
        return null;
    }

    // ------------------ DB helpers ------------------

    private static Connection connect() throws SQLException {
        String url = "jdbc:mysql://" + DB_HOST + ":" + DB_PORT + "/" + DB_NAME
                + "?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true";
        Properties props = new Properties();
        props.setProperty("user", DB_USER);
        props.setProperty("password", DB_PASS);
        props.setProperty("useCursorFetch", "true");
        Connection c = DriverManager.getConnection(url, props);
        return c;
    }
}
