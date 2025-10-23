package org.pvlens.spl.processing.merge;

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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import org.pvlens.spl.om.SplDrug;
import org.pvlens.spl.om.Srlc;

public class SrlcMerge {

	/**
	 * Updates label data from the Safety-related label changes (SRLCs).
	 *
	 * @param allProducts the collection of all SplDrug products
	 * @param srlcs       the list of Safety-related label changes
	 */
	public static void updateLabelsFromSrlc(ConcurrentLinkedQueue<SplDrug> allProducts, List<Srlc> srlcs) {
		// Create a lookup table for quick access to SRLCs by application number (NDA)
		Map<Integer, List<Srlc>> ndaLookup = srlcs.stream().filter(srlc -> srlc.getApplicationNumber() > 0)
				.collect(Collectors.groupingBy(Srlc::getApplicationNumber));

		// Iterate over all SplDrug products
		for (SplDrug drug : allProducts) {
			List<Integer> ndaIds = drug.getNdaIds();
			if (ndaIds == null || ndaIds.isEmpty()) {
				continue;
			}

			// For each NDA ID associated with the drug
			for (int ndaId : ndaIds) {
				List<Srlc> mappedSrlcs = ndaLookup.get(ndaId);
				if (mappedSrlcs == null) {
					continue;
				}

				// Update the drug with each relevant SRLC
				for (Srlc srlc : mappedSrlcs) {
					if (srlc.hasCodes()) {
						drug.updateLabels(srlc);
						// Associate the SRLC with the drug
						drug.getSrlcs().putIfAbsent(ndaId, srlc);
					}
				}
			}
		}
	}
}
