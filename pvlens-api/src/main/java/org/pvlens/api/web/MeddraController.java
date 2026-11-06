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

import org.pvlens.api.model.MeddraTerm;
import org.pvlens.api.model.Page;
import org.pvlens.api.model.ProductHit;
import org.pvlens.api.repository.PvlensRepository;
import org.pvlens.api.repository.PvlensRepository.Role;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

@RestController
@RequestMapping("/v1/meddra")
@Tag(name = "MedDRA")
public class MeddraController {

    private final PvlensRepository repo;

    public MeddraController(PvlensRepository repo) {
        this.repo = repo;
    }

    @GetMapping
    @Operation(summary = "Find MedDRA terms by text, shortest match first")
    public List<MeddraTerm> search(
            @RequestParam @Size(min = 2, max = 200) String q,
            @Parameter(description = "Term type, e.g. PT or LLT")
            @RequestParam(required = false) @Size(max = 10) String tty,
            @RequestParam(defaultValue = "25") @Min(1) @Max(200) int limit) {
        return repo.searchMeddra(q, tty, limit);
    }

    @GetMapping("/{code}/products")
    @Operation(summary = "Products whose label lists this MedDRA code",
            description = "A preferred term (PT) code also matches products labelled with its lowest-level terms.")
    public Page<ProductHit> products(
            @PathVariable @Pattern(regexp = "\\d{1,12}", message = "MedDRA codes are numeric") String code,
            @Parameter(description = "ae (default) or indication")
            @RequestParam(defaultValue = "ae") String role,
            @Parameter(description = "Adverse events only: filter on boxed warning")
            @RequestParam(required = false) Boolean boxedWarning,
            @RequestParam(defaultValue = "0") @Min(0) int page,
            @RequestParam(defaultValue = "100") @Min(1) @Max(1000) int size) {

        Role r = switch (role.toLowerCase(Locale.ROOT)) {
            case "ae", "adverse-event" -> Role.AE;
            case "indication" -> Role.INDICATION;
            default -> throw new IllegalArgumentException("role must be ae or indication");
        };
        return repo.productsForMeddra(code, r, boxedWarning, page, size);
    }
}
