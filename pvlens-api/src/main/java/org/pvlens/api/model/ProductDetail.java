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

import java.util.List;

/** Full description of a PVLens product. */
public record ProductDetail(
        long id,
        List<String> names,
        List<Ndc> ndcs,
        List<Concept> rxnorm,
        List<Concept> atc,
        List<Concept> snomedIngredients,
        List<LabelSource> labels) {
}
