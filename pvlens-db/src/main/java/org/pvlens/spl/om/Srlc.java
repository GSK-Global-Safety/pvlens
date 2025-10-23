package org.pvlens.spl.om;

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

import java.util.Date;
import java.util.HashMap;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.pvlens.spl.umls.Atom;

import lombok.Data;

@Data
public class Srlc {

	private String drugName;
	private String activeIngredient;
	private int applicationNumber;
	private Date supplementDate;
	private Date databaseUpdated;
	private String url;

	// Outcomes captured in processing the data
	private Outcome exactAeMatch;
	private Outcome nlpAeMatch;
	private Outcome exactBlackboxMatch;
	private Outcome nlpBlackboxMatch;

	public Srlc() {
		this.exactAeMatch = new Outcome();
		this.nlpAeMatch = new Outcome();
		this.exactBlackboxMatch = new Outcome();
		this.nlpBlackboxMatch = new Outcome();
	}

	/**
	 * Robustly parse drug id from the SRLC URL's query string. Accepts either
	 * "...?foo=bar&drug_id=123&..." or the prior fixed-position format used before.
	 * Returns -1 on any parse issue.
	 */
	public int getDrugId() { // NEW: robust + exception-safe
		if (StringUtils.isBlank(this.url))
			return -1;
		try {
			// normalize separators and split params
			String[] parts = this.url.split("\\?");
			String query = (parts.length > 1) ? parts[1] : "";
			if (StringUtils.isBlank(query))
				return -1;

			String[] pairs = query.split("&");
			for (String p : pairs) {
				String[] kv = p.split("=", 2);
				if (kv.length == 2) {
					String key = kv[0].trim().toLowerCase();
					String val = kv[1].trim();
					if ("drug_id".equals(key) || "drugid".equals(key) || "id".equals(key)) {
						return Integer.parseInt(val);
					}
				}
			}

			// fallback to legacy assumption: drug id in second pair
			if (pairs.length > 1 && pairs[1].contains("=")) {
				return Integer.parseInt(pairs[1].split("=")[1].trim());
			}
		} catch (Exception ignore) {
			// swallow and fall through
		}
		return -1;
	}

	public boolean hasCodes() {
		return !this.exactAeMatch.getCodes().isEmpty() || !this.nlpAeMatch.getCodes().isEmpty()
				|| !this.exactBlackboxMatch.getCodes().isEmpty() || !this.nlpBlackboxMatch.getCodes().isEmpty();
	}

	/**
	 * Deduplicate NLP outcomes against exact outcomes and prune firstAdded for any
	 * codes that were removed.
	 */
	public void resolveLabeledEvents() {
		if (exactAeMatch == null || exactBlackboxMatch == null || nlpAeMatch == null || nlpBlackboxMatch == null) {
			return; // NEW: null-safe guard
		}

		// Remove exact matches from NLP matches
		Set<Atom> exactBlackBox = exactBlackboxMatch.getCodes();
		nlpBlackboxMatch.getCodes().removeIf(exactBlackBox::contains);

		Set<Atom> exactWarning = exactAeMatch.getCodes();
		nlpAeMatch.getCodes().removeIf(exactWarning::contains);

		// Once resolved, remove any missing codes from first added
		cleanupFirstAdded();
	}

	private void cleanupFirstAdded() {
		Outcome[] outcomes = new Outcome[] { exactAeMatch, exactBlackboxMatch, nlpAeMatch, nlpBlackboxMatch };

		for (Outcome outcome : outcomes) {
			HashMap<String, Date> priorFirstAdded = outcome.getFirstAdded();
			HashMap<String, Date> updatedFirstAdded = new HashMap<>();

			for (String aui : priorFirstAdded.keySet()) {
				for (Atom code : outcome.getCodes()) {
					if (aui.equals(code.getAui())) {
						updatedFirstAdded.put(aui, priorFirstAdded.get(aui));
						break;
					}
				}
			}
			outcome.setFirstAdded(updatedFirstAdded);
		}
	}
}
