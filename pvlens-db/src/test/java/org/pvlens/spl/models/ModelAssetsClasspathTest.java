package org.pvlens.spl.models;

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


/*
 * Unit test to ensure all NLP models/dicts are present on the classpath
 * and (for binary models) can be successfully loaded by OpenNLP.
 *
 * Files expected under: src/main/resources/models/
 *
 * - en-chunker.bin               -> ChunkerModel
 * - en-lemmatizer.dict           -> (dictionary file; presence + non-empty check)
 * - en-pos-maxent.bin            -> POSModel
 * - en-sent.bin                  -> SentenceModel
 * - en-token.bin                 -> TokenizerModel
 * - meddra-section-doccat.bin    -> DoccatModel
 */
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import opennlp.tools.chunker.ChunkerModel;
import opennlp.tools.doccat.DoccatModel;
import opennlp.tools.postag.POSModel;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.tokenize.TokenizerModel;

@Tag("unit")
public class ModelAssetsClasspathTest {

  private static final String BASE = "/models/";

  @Test
  void allModels_ArePresent_AndLoadable() throws Exception {
    // (name -> loader)
    Map<String, Loader> checks = new LinkedHashMap<>();
    checks.put("en-chunker.bin", in -> new ChunkerModel(in));
    checks.put("en-lemmatizer.dict", this::justRead);         // dict (non-binary model): presence + non-empty
    checks.put("en-pos-maxent.bin", in -> new POSModel(in));
    checks.put("en-sent.bin", in -> new SentenceModel(in));
    checks.put("en-token.bin", in -> new TokenizerModel(in));
    checks.put("meddra-section-doccat.bin", in -> new DoccatModel(in));

    for (Map.Entry<String, Loader> e : checks.entrySet()) {
      String resource = BASE + e.getKey();
      try (InputStream in = mustGet(resource)) {
        // Ensure we can load/parse (for dict, `justRead` will fully consume stream)
        Object model = e.getValue().load(in);
        assertNotNull(model, "Model loader returned null for: " + resource);
      } catch (Exception ex) {
        fail("Failed to load resource " + resource + ": " + ex.getMessage(), ex);
      }
    }
  }

  // --- helpers ---

  /** Ensure resource exists on classpath; throw with a clear message if not found. */
  private InputStream mustGet(String absoluteClasspathPath) throws IOException {
    InputStream in = getClass().getResourceAsStream(absoluteClasspathPath);
    if (in == null) {
      throw new IOException("Missing resource on classpath: " + absoluteClasspathPath
          + "  (Expected under src/main/resources" + absoluteClasspathPath + ")");
    }
    // Quick non-empty sanity check
    if (in.available() == 0) {
      // Some streams may return 0 for available() even when readable, so don't fail here—
      // the specific loader/read below will verify content properly.
    }
    return in;
  }

  /** For plain dictionary files: read a few bytes to ensure it's non-empty. */
  private Object justRead(InputStream in) throws IOException {
    byte[] buf = new byte[256];
    int n = in.read(buf);
    if (n <= 0) {
      throw new IOException("Dictionary file appears empty");
    }
    // Not a model object—return a sentinel
    return Boolean.TRUE;
  }

  @FunctionalInterface
  private interface Loader {
    Object load(InputStream in) throws Exception;
  }
}
