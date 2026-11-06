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

import java.util.List;
import java.util.Locale;

import org.pvlens.api.model.LabelSection;
import org.pvlens.api.model.LabelSource;
import org.pvlens.api.model.Section;
import org.pvlens.api.repository.PvlensRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.constraints.Size;

@RestController
@RequestMapping("/v1/labels")
@Tag(name = "Labels")
public class LabelController {

    private final PvlensRepository repo;

    public LabelController(PvlensRepository repo) {
        this.repo = repo;
    }

    public record LabelInfo(String guid, List<Long> productIds, List<LabelSource> files) {
    }

    @GetMapping("/{guid}")
    @Operation(summary = "SPL label metadata and the products it belongs to")
    public LabelInfo get(@PathVariable @Size(max = 500) String guid) {
        List<LabelSource> files = repo.labelsByGuid(guid);
        if (files.isEmpty()) {
            throw new NotFoundException("No label with GUID " + guid);
        }
        return new LabelInfo(guid, repo.productIdsForGuid(guid), files);
    }

    @GetMapping("/{guid}/sections/{section}")
    @Operation(summary = "Raw text of a label section, newest label version first")
    public List<LabelSection> section(
            @PathVariable @Size(max = 500) String guid,
            @Parameter(description = "adverse-reactions, indications or boxed-warning")
            @PathVariable String section) {

        Section s = switch (section.toLowerCase(Locale.ROOT)) {
            case "adverse-reactions" -> Section.ADVERSE_REACTIONS;
            case "indications" -> Section.INDICATIONS;
            case "boxed-warning" -> Section.BOXED_WARNING;
            default -> throw new IllegalArgumentException(
                    "section must be adverse-reactions, indications or boxed-warning");
        };
        List<LabelSection> out = repo.labelSections(guid, s);
        if (out.isEmpty()) {
            throw new NotFoundException("No " + section + " text for label " + guid);
        }
        return out;
    }
}
