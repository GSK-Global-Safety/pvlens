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

/** Label sections whose raw text PVLens stores. */
public enum Section {
    ADVERSE_REACTIONS("SPL_AE_TEXT"),
    INDICATIONS("SPL_IND_TEXT"),
    BOXED_WARNING("SPL_BOX_TEXT");

    private final String table;

    Section(String table) {
        this.table = table;
    }

    /** Table holding this section. Fixed per constant, never user input. */
    public String table() {
        return table;
    }
}
