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

import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;

import org.pvlens.spl.conf.ConfigLoader;
import org.pvlens.spl.nlp.CharNGramFeatureGenerator;

import opennlp.tools.doccat.BagOfWordsFeatureGenerator;
import opennlp.tools.doccat.DoccatFactory;
import opennlp.tools.doccat.DoccatModel;
import opennlp.tools.doccat.DocumentCategorizerME;
import opennlp.tools.doccat.DocumentSample;
import opennlp.tools.doccat.FeatureGenerator;
import opennlp.tools.doccat.NGramFeatureGenerator;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.tokenize.SimpleTokenizer;
import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.util.ObjectStream;
import opennlp.tools.util.TrainingParameters;
import opennlp.tools.util.model.ModelUtil;

/**
 * Build model for doccat from original SPL label sources
 */
public class BuildSectionDoccatModel {

	// ---------- Config / flags ----------
	private static final String SENT_MODEL_PATH = "models/en-sent.bin";
	private static final String TOKEN_MODEL_PATH = "models/en-token.bin";

	// Update model output path
	private static final String OUTPUT_MODEL_PATH = "src/main/resources/models/meddra-section-doccat.bin";

	// Sampling / filtering (safe defaults)
	private static final int MAX_PER_CLASS = 80_000;
	private static final int MIN_TOKENS_SENT = 4;
	private static final boolean DEDUP_SENT = true;

	// Optional WHERE clauses or date filters (empty => all)
	private static final String DATE_SINCE = "2015-01-01";

	// ---------- DB (via your ConfigLoader) ----------
	private static String DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME;

	// ---------- Lazy NLP ----------
	private static SentenceDetectorME SENT_DET;
	private static Tokenizer TOKENIZER;

	// ---------- Labels ----------
	private enum Label {
		IND, AE, BOX
	}

	public static void main(String[] args) throws Exception {
		// Load DB config from your existing loader
		ConfigLoader cfg = new ConfigLoader();
		DB_HOST = cfg.getDbHost();
		DB_PORT = cfg.getDbPort();
		DB_USER = cfg.getDbUser();
		DB_PASS = cfg.getDbPass();
		DB_NAME = cfg.getDbName();

		// Init optional NLP models
		SENT_DET = loadSentenceModel(SENT_MODEL_PATH);
		TOKENIZER = loadTokenizer(TOKEN_MODEL_PATH);

		System.out.printf("Doccat training start. sentModel=%s, tokModel=%s%n", (SENT_DET != null),
				(TOKENIZER != null));

		try (Connection conn = connect()) {
			// Build an ObjectStream<DocumentSample> that streams all labeled sentences
			ObjectStream<DocumentSample> stream = buildTrainingStream(conn);

			// Train
			Instant t0 = Instant.now();
			DoccatModel model = train(stream);
			Instant t1 = Instant.now();

			// Save
			try (OutputStream out = new BufferedOutputStream(new FileOutputStream(OUTPUT_MODEL_PATH))) {
				model.serialize(out);
			}
			System.out.printf("Model written to %s (%.1fs)%n", OUTPUT_MODEL_PATH,
					Duration.between(t0, t1).toMillis() / 1000.0);
		}
	}

	// ------------------ Training ------------------

	private static DoccatModel train(ObjectStream<DocumentSample> samples) throws IOException {
		// Feature generators: bag-of-words + word n-grams + custom char n-grams
		FeatureGenerator[] fgs = new FeatureGenerator[] { new BagOfWordsFeatureGenerator(),
				new NGramFeatureGenerator(1, 2), new org.pvlens.spl.nlp.CharNGramFeatureGenerator(3, 5) };

		TrainingParameters params = ModelUtil.createDefaultTrainingParameters();
		params.put(TrainingParameters.CUTOFF_PARAM, Integer.toString(2));
		params.put(TrainingParameters.ITERATIONS_PARAM, Integer.toString(30));

		// IMPORTANT: pass the feature generators via the factory
		DoccatFactory factory = new DoccatFactory(fgs);

		return DocumentCategorizerME.train("en", samples, params, factory);
	}

	// ------------------ Data stream builder ------------------

	private static ObjectStream<DocumentSample> buildTrainingStream(Connection conn) {
		List<LabeledRowSource> sources = new ArrayList<>();

		// Optional date filter
		String dateFilter = (DATE_SINCE == null || DATE_SINCE.isBlank()) ? "" : " AND LABEL_DATE >= ?";
		sources.add(new LabeledRowSource(conn, "SPL_IND_TEXT", "SPL_TEXT", Label.IND, MAX_PER_CLASS, dateFilter));
		sources.add(new LabeledRowSource(conn, "SPL_AE_TEXT", "SPL_TEXT", Label.AE, MAX_PER_CLASS, dateFilter));
		sources.add(new LabeledRowSource(conn, "SPL_BOX_TEXT", "SPL_TEXT", Label.BOX, MAX_PER_CLASS, dateFilter));

		// Wrap each table as a DocumentSample stream, then concatenate
		List<ObjectStream<DocumentSample>> perTableStreams = new ArrayList<>();
		for (LabeledRowSource src : sources) {
			perTableStreams.add(src.stream());
		}
		return new ConcatenatedObjectStream<>(perTableStreams);
	}

	// ------------------ Per-table streaming ------------------

	/**
	 * Streams rows from a table, splits into sentences, filters, tokenizes, yields
	 * DocumentSample(label, tokens[])
	 */
	private static final class LabeledRowSource {
		private final Connection conn;
		private final String table;
		private final String textCol;
		private final Label label;
		private final int cap;
		private final String dateFilter;

		LabeledRowSource(Connection conn, String table, String textCol, Label label, int cap, String dateFilter) {
			this.conn = conn;
			this.table = table;
			this.textCol = textCol;
			this.label = label;
			this.cap = cap;
			this.dateFilter = dateFilter;
		}

		ObjectStream<DocumentSample> stream() {
			return new ObjectStream<>() {
				private PreparedStatement ps;
				private ResultSet rs;
				private Deque<DocumentSample> buffer = new ArrayDeque<>();
				private int emitted = 0;
				private final Set<String> seen = DEDUP_SENT ? new HashSet<>(8192) : Collections.emptySet();

				@Override
				public DocumentSample read() throws IOException {
					try {
						while (true) {
							if (!buffer.isEmpty())
								return buffer.removeFirst();
							if (emitted >= cap)
								return null;

							if (rs == null) {
								initQuery();
							}
							if (!rs.next()) {
								close();
								return null;
							}
							String text = rs.getString(textCol);
							if (text == null || text.isBlank())
								continue;

							// Sentence split
							String[] sentences = detectSentences(text);
							for (String s : sentences) {
								String normalized = normalize(s);
								if (normalized.isEmpty())
									continue;

								String[] tokens = tokenize(normalized);
								if (tokens.length < MIN_TOKENS_SENT)
									continue;

								// Dedup identical sentences (optional)
								if (DEDUP_SENT) {
									String key = normalized;
									if (!seen.add(key))
										continue;
								}

								buffer.addLast(new DocumentSample(label.name(), tokens));
								emitted++;
								if (emitted >= cap)
									break;
							}
						}
					} catch (SQLException e) {
						throw new IOException("SQL error reading " + table, e);
					}
				}

				@Override
				public void reset() throws IOException, UnsupportedOperationException {
					close(); // fresh query on next read()
				}

				@Override
				public void close() throws IOException {
					try {
						if (rs != null)
							rs.close();
					} catch (SQLException ignore) {
					}
					try {
						if (ps != null)
							ps.close();
					} catch (SQLException ignore) {
					}
					rs = null;
					ps = null;
					buffer.clear();
				}

				private void initQuery() throws SQLException {
					String sql = "SELECT " + textCol + " FROM " + table + " WHERE 1=1 " + dateFilter;
					sql += " ORDER BY ID"; // deterministic; you can change to RAND() if you prefer random sampling
					if (cap < Integer.MAX_VALUE)
						sql += " LIMIT " + (cap * 2L); // oversample a bit to allow filtering
					ps = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
					ps.setFetchSize(Integer.MIN_VALUE); // MySQL streaming (server-side)
					if (!dateFilter.isBlank()) {
						ps.setString(1, DATE_SINCE);
					}
					rs = ps.executeQuery();
					System.out.printf("Streaming %s (label=%s)%n", table, label);
				}
			};
		}
	}

	// ------------------ NLP helpers ------------------

	private static SentenceDetectorME loadSentenceModel(String path) {
		try (InputStream in = tryOpen(path)) {
			if (in == null)
				return null;
			return new SentenceDetectorME(new SentenceModel(in));
		} catch (Exception e) {
			return null;
		}
	}

	private static Tokenizer loadTokenizer(String path) {
		try (InputStream in = tryOpen(path)) {
			if (in == null)
				return SimpleTokenizer.INSTANCE; // fallback
			return new TokenizerME(new TokenizerModel(in));
		} catch (Exception e) {
			return SimpleTokenizer.INSTANCE;
		}
	}

	private static String[] detectSentences(String text) {
		if (SENT_DET != null) {
			try {
				return SENT_DET.sentDetect(text);
			} catch (Exception ignore) {
			}
		}
		// conservative regex fallback
		return text.split("(?<=[.!?])\\s+");
	}

	private static String[] tokenize(String sentence) {
		if (TOKENIZER != null) {
			try {
				return TOKENIZER.tokenize(sentence);
			} catch (Exception ignore) {
			}
		}
		return sentence.split("\\s+");
	}

	// mirror your runtime normalization basics (lowercase + light punctuation
	// folding)
	private static String normalize(String s) {
		if (s == null)
			return "";
		String out = s.toLowerCase(Locale.ROOT).trim();
		if (out.isEmpty())
			return out;

		// unify curly quotes/dashes and collapse common artifacts
		out = out.replace('\u2019', '\'').replace('\u2018', '\'').replace('\u201c', '"').replace('\u201d', '"')
				.replace('\u2013', '-').replace('\u2014', '-').replace('\u2212', '-');
		// collapse "' s" -> "'s" and hyphenated words to spaces between letters
		out = out.replaceAll("\\s*'\\s*s\\b", "'s");
		out = out.replaceAll("(?<=\\p{L})-(?=\\p{L})", " ");
		out = out.replaceAll("\\s+(['-])\\s+", "$1");
		return out;
	}

	private static InputStream tryOpen(String path) {
		// FS first
		try {
			java.nio.file.Path p = java.nio.file.Paths.get(path);
			if (java.nio.file.Files.exists(p))
				return new FileInputStream(p.toFile());
		} catch (Exception ignore) {
		}
		// then classpath
		try {
			InputStream in = BuildSectionDoccatModel.class.getClassLoader().getResourceAsStream(path);
			if (in != null)
				return in;
		} catch (Exception ignore) {
		}
		return null;
	}

	// ------------------ DB helpers ------------------

	private static Connection connect() throws SQLException {
		String url = "jdbc:mysql://" + DB_HOST + ":" + DB_PORT + "/" + DB_NAME
				+ "?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true";
		Properties props = new Properties();
		props.setProperty("user", DB_USER);
		props.setProperty("password", DB_PASS);
		// Streaming mode for large tables
		props.setProperty("useCursorFetch", "true");
		Connection c = DriverManager.getConnection(url, props);
		return c;
	}

	// ------------------ Concatenated stream ------------------

	/** Concatenates multiple ObjectStreams in sequence. */
	private static final class ConcatenatedObjectStream<T> implements ObjectStream<T> {
		private final Iterator<ObjectStream<T>> it;
		private ObjectStream<T> current;

		ConcatenatedObjectStream(List<ObjectStream<T>> streams) {
			this.it = streams.iterator();
		}

		@Override
		public T read() throws IOException {
			while (true) {
				if (current == null) {
					if (!it.hasNext())
						return null;
					current = it.next();
				}
				T v = current.read();
				if (v != null)
					return v;
				current.close();
				current = null;
			}
		}

		@Override
		public void reset() throws IOException {
			throw new UnsupportedOperationException("reset not supported");
		}

		@Override
		public void close() throws IOException {
			if (current != null)
				current.close();
			while (it.hasNext())
				it.next().close();
		}
	}
}
