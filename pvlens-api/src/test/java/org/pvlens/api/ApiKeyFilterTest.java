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

package org.pvlens.api;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.pvlens.api.config.ApiKeyFilter;
import org.pvlens.api.config.ApiProperties;

class ApiKeyFilterTest {

    @Test
    void refusesToStartWithoutKeys() {
        assertThatThrownBy(() -> new ApiKeyFilter(new ApiProperties(List.of(" "), false)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("No API keys configured");
    }

    @Test
    void anonymousModeNeedsNoKeys() {
        new ApiKeyFilter(new ApiProperties(null, true));
    }
}
