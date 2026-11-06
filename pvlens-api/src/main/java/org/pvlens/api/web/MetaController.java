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

package org.pvlens.api.web;

import org.pvlens.api.model.Meta;
import org.pvlens.api.repository.PvlensRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;

@RestController
@Tag(name = "Meta")
public class MetaController {

    private final PvlensRepository repo;

    public MetaController(PvlensRepository repo) {
        this.repo = repo;
    }

    @GetMapping("/v1/meta")
    @Operation(summary = "UMLS version and size of the database being served")
    public Meta meta() {
        return new Meta("v1", repo.umlsVersions(), repo.productCount());
    }
}
