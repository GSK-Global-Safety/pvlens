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

/** A product that lists a given MedDRA term, returned by reverse lookups. */
public record ProductHit(
        long productId,
        List<String> productNames,
        MeddraTerm meddra,
        LocalDate firstLabelDate,
        Boolean warning,
        Boolean boxedWarning,
        boolean exactMatch) {
}
