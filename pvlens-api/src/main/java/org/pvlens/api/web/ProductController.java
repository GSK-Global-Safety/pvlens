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

import java.time.LocalDate;
import java.util.List;

import org.pvlens.api.model.LabelChange;
import org.pvlens.api.model.LabeledTerm;
import org.pvlens.api.model.Page;
import org.pvlens.api.model.ProductDetail;
import org.pvlens.api.model.ProductSummary;
import org.pvlens.api.repository.PvlensRepository;
import org.pvlens.api.repository.PvlensRepository.AeFilter;
import org.pvlens.api.repository.PvlensRepository.ProductQuery;
import org.springframework.format.annotation.DateTimeFormat;
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
import jakarta.validation.constraints.Size;

@RestController
@RequestMapping("/v1/products")
@Tag(name = "Products")
public class ProductController {

    private final PvlensRepository repo;

    public ProductController(PvlensRepository repo) {
        this.repo = repo;
    }

    @GetMapping
    @Operation(summary = "Search products",
            description = "At least one filter is required. Filters combine with AND.")
    public Page<ProductSummary> search(
            @Parameter(description = "Part of a product or RxNorm name, case-insensitive")
            @RequestParam(required = false) @Size(min = 2, max = 200) String name,
            @Parameter(description = "Exact NDC code as printed on the label, e.g. 0173-0712")
            @RequestParam(required = false) @Size(max = 50) String ndc,
            @Parameter(description = "RxNorm concept code (RxCUI)")
            @RequestParam(required = false) @Size(max = 20) String rxcui,
            @Parameter(description = "ATC code or class prefix, e.g. C09 or C09AA02")
            @RequestParam(required = false) @Size(max = 20) String atc,
            @RequestParam(defaultValue = "0") @Min(0) int page,
            @RequestParam(defaultValue = "50") @Min(1) @Max(500) int size) {

        ProductQuery q = new ProductQuery(name, ndc, rxcui, atc);
        if (q.isEmpty()) {
            throw new IllegalArgumentException("Give at least one of: name, ndc, rxcui, atc");
        }
        return repo.searchProducts(q, page, size);
    }

    @GetMapping("/{id}")
    @Operation(summary = "Product detail: names, NDCs, RxNorm, ATC, SNOMED ingredients and source labels")
    public ProductDetail get(@PathVariable long id) {
        return repo.findProduct(id).orElseThrow(() -> notFound(id));
    }

    @GetMapping("/{id}/adverse-events")
    @Operation(summary = "Adverse events on the product label, mapped to MedDRA")
    public Page<LabeledTerm> adverseEvents(
            @PathVariable long id,
            @Parameter(description = "Only terms in (true) or not in (false) a boxed warning")
            @RequestParam(required = false) Boolean boxedWarning,
            @Parameter(description = "Only terms in (true) or not in (false) the Warnings section")
            @RequestParam(required = false) Boolean warning,
            @Parameter(description = "Only exact (true) or NLP-matched (false) mappings")
            @RequestParam(required = false) Boolean exactMatch,
            @Parameter(description = "Only terms first labelled on or after this date (yyyy-MM-dd)")
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate since,
            @RequestParam(defaultValue = "0") @Min(0) int page,
            @RequestParam(defaultValue = "100") @Min(1) @Max(1000) int size) {

        requireProduct(id);
        return repo.adverseEvents(id, new AeFilter(boxedWarning, warning, exactMatch, since), page, size);
    }

    @GetMapping("/{id}/indications")
    @Operation(summary = "Indications on the product label, mapped to MedDRA")
    public Page<LabeledTerm> indications(
            @PathVariable long id,
            @RequestParam(required = false) Boolean exactMatch,
            @RequestParam(defaultValue = "0") @Min(0) int page,
            @RequestParam(defaultValue = "100") @Min(1) @Max(1000) int size) {

        requireProduct(id);
        return repo.indications(id, exactMatch, page, size);
    }

    @GetMapping("/{id}/label-changes")
    @Operation(summary = "FDA Safety Related Label Changes (SRLC) for the product")
    public List<LabelChange> labelChanges(@PathVariable long id) {
        requireProduct(id);
        return repo.labelChanges(id);
    }

    private void requireProduct(long id) {
        if (!repo.productExists(id)) {
            throw notFound(id);
        }
    }

    private static NotFoundException notFound(long id) {
        return new NotFoundException("No product with id " + id);
    }
}
