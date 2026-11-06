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

package org.pvlens.api.config;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * API settings under {@code pvlens.api}.
 *
 * @param keys           accepted values of the X-API-Key header
 * @param allowAnonymous serve /v1 without a key; only for local development
 */
@ConfigurationProperties(prefix = "pvlens.api")
public record ApiProperties(List<String> keys, boolean allowAnonymous) {

    public ApiProperties {
        keys = keys == null ? List.of()
                : keys.stream().filter(k -> k != null && !k.isBlank()).map(String::trim).toList();
    }
}
