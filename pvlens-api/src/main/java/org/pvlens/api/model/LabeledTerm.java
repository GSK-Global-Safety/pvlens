/*
 * This file is part of PVLens.
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

package org.pvlens.api.model;

import java.time.LocalDate;
import java.util.List;

/**
 * A MedDRA term found on a product label, either as an adverse event or as an
 * indication. The warning and boxedWarning flags are only set for adverse events.
 */
public record LabeledTerm(
        MeddraTerm meddra,
        LocalDate firstLabelDate,
        Boolean warning,
        Boolean boxedWarning,
        boolean exactMatch,
        List<String> sourceLabelGuids) {
}
