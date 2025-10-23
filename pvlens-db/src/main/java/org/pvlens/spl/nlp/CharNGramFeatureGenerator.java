package org.pvlens.spl.nlp;


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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import opennlp.tools.doccat.FeatureGenerator;

/**
 * Public, top-level generator so OpenNLP can reload it from serialized models.
 */
public class CharNGramFeatureGenerator implements FeatureGenerator {

	private final int min;
	private final int max;

	/** No-arg constructor required for OpenNLP extension loader; defaults 3..5. */
	public CharNGramFeatureGenerator() {
		this(3, 5);
	}

	public CharNGramFeatureGenerator(int min, int max) {
		this.min = Math.max(1, min);
		this.max = Math.max(this.min, max);
	}

	@Override
	public Collection<String> extractFeatures(String[] tokens, Map<String, Object> extraInformation) {
		List<String> features = new ArrayList<>();
		if (tokens == null)
			return features;

		for (String tok : tokens) {
			if (tok == null)
				continue;
			int len = tok.length();
			for (int n = min; n <= max; n++) {
				if (len < n)
					continue;
				for (int i = 0; i + n <= len; i++) {
					features.add("charng=" + n + ":" + tok.substring(i, i + n));
				}
			}
		}
		return features;
	}
}
