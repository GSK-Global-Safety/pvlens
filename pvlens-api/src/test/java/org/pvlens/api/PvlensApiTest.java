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

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;

/** End-to-end tests against the real PVLens schema loaded into H2. */
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
class PvlensApiTest {

    @Autowired
    private MockMvc mvc;

    private ResultActions call(String url) throws Exception {
        return mvc.perform(get(url).header("X-API-Key", "test-key-2"));
    }

    /* ---------------------------------- Auth ---------------------------------- */

    @Test
    void rejectsMissingKey() throws Exception {
        mvc.perform(get("/v1/meta")).andExpect(status().isUnauthorized());
    }

    @Test
    void rejectsWrongKey() throws Exception {
        mvc.perform(get("/v1/meta").header("X-API-Key", "nope")).andExpect(status().isUnauthorized());
    }

    @Test
    void healthAndDocsAreOpen() throws Exception {
        mvc.perform(get("/actuator/health")).andExpect(status().isOk());
        mvc.perform(get("/v3/api-docs"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.paths['/v1/products']").exists());
    }

    /* ------------------------------- Products ------------------------------- */

    @Test
    void meta() throws Exception {
        call("/v1/meta")
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.umlsVersions", contains("2025A")))
                .andExpect(jsonPath("$.productCount").value(3));
    }

    @Test
    void searchByNameMatchesNdcAndRxnormNames() throws Exception {
        call("/v1/products?name=AVODART")
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").value(1))
                .andExpect(jsonPath("$.items[0].id").value(1))
                .andExpect(jsonPath("$.items[0].names", contains("Avodart", "Avodart 0.5 mg")));

        call("/v1/products?name=primid")
                .andExpect(jsonPath("$.items[0].id").value(2));
    }

    @Test
    void searchTreatsLikeWildcardsLiterally() throws Exception {
        mvc.perform(get("/v1/products").param("name", "50%").header("X-API-Key", "test-key-1"))
                .andExpect(jsonPath("$.total").value(1));
        mvc.perform(get("/v1/products").param("name", "a%t").header("X-API-Key", "test-key-1"))
                .andExpect(jsonPath("$.total").value(0));
        mvc.perform(get("/v1/products").param("name", "av_dart").header("X-API-Key", "test-key-1"))
                .andExpect(jsonPath("$.total").value(0));
    }

    @Test
    void searchByNdcRxcuiAndAtcPrefix() throws Exception {
        call("/v1/products?ndc=0000-0002").andExpect(jsonPath("$.items[0].id").value(2));
        call("/v1/products?rxcui=228790").andExpect(jsonPath("$.items[0].id").value(1));
        call("/v1/products?atc=n03").andExpect(jsonPath("$.items[0].id").value(2));
        call("/v1/products?atc=G04&name=primidone").andExpect(jsonPath("$.total").value(0));
    }

    @Test
    void searchNeedsAFilter() throws Exception {
        call("/v1/products").andExpect(status().isBadRequest());
    }

    @Test
    void searchPaginates() throws Exception {
        call("/v1/products?name=id&size=1&page=1")
                .andExpect(jsonPath("$.total").value(2))
                .andExpect(jsonPath("$.items", hasSize(1)))
                .andExpect(jsonPath("$.items[0].id").value(2));
        call("/v1/products?name=avodart&size=0").andExpect(status().isBadRequest());
    }

    @Test
    void productDetail() throws Exception {
        call("/v1/products/1")
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.ndcs", hasSize(2)))
                .andExpect(jsonPath("$.rxnorm[0].code").value("228790"))
                .andExpect(jsonPath("$.atc", hasSize(1)))
                .andExpect(jsonPath("$.atc[0].code").value("G04CB02"))
                .andExpect(jsonPath("$.snomedIngredients[0].term").value("Dutasteride"))
                .andExpect(jsonPath("$.labels", hasSize(2)))
                .andExpect(jsonPath("$.labels[0].sourceType").value("prescription"))
                .andExpect(jsonPath("$.labels[0].applicationNumber").value(21319));
    }

    @Test
    void productDetailHidesZeroApplicationNumber() throws Exception {
        call("/v1/products/2").andExpect(jsonPath("$.labels[0].applicationNumber").doesNotExist());
    }

    @Test
    void unknownProductIs404() throws Exception {
        call("/v1/products/999").andExpect(status().isNotFound());
        call("/v1/products/999/adverse-events").andExpect(status().isNotFound());
    }

    /* ----------------------- Adverse events / indications ----------------------- */

    @Test
    void adverseEvents() throws Exception {
        call("/v1/products/1/adverse-events")
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").value(2))
                .andExpect(jsonPath("$.items[0].meddra.term").value("Hepatotoxicity"))
                .andExpect(jsonPath("$.items[0].boxedWarning").value(true))
                .andExpect(jsonPath("$.items[0].sourceLabelGuids", contains("guid-avodart-2")))
                .andExpect(jsonPath("$.items[1].meddra.term").value("Impotence"))
                .andExpect(jsonPath("$.items[1].meddra.ptCode").value("10061461"))
                .andExpect(jsonPath("$.items[1].firstLabelDate").value("2002-01-01"))
                .andExpect(jsonPath("$.items[1].sourceLabelGuids", contains("guid-avodart", "guid-avodart-2")));
    }

    @Test
    void adverseEventFilters() throws Exception {
        call("/v1/products/1/adverse-events?boxedWarning=true")
                .andExpect(jsonPath("$.total").value(1))
                .andExpect(jsonPath("$.items[0].meddra.code").value("10019851"));
        call("/v1/products/1/adverse-events?exactMatch=true")
                .andExpect(jsonPath("$.items[0].meddra.term").value("Impotence"));
        call("/v1/products/1/adverse-events?since=2005-01-01")
                .andExpect(jsonPath("$.total").value(1));
        call("/v1/products/1/adverse-events?since=not-a-date").andExpect(status().isBadRequest());
    }

    @Test
    void indicationsHaveNoWarningFlags() throws Exception {
        call("/v1/products/1/indications")
                .andExpect(jsonPath("$.items[0].meddra.term").value("Benign prostatic hyperplasia"))
                .andExpect(jsonPath("$.items[0].boxedWarning").doesNotExist())
                .andExpect(jsonPath("$.items[0].sourceLabelGuids", contains("guid-avodart")));
    }

    @Test
    void labelChanges() throws Exception {
        call("/v1/products/1/label-changes")
                .andExpect(jsonPath("$[0].drugName").value("AVODART"))
                .andExpect(jsonPath("$[0].supplementDate").value("2011-06-14"));
        call("/v1/products/2/label-changes").andExpect(jsonPath("$", hasSize(0)));
    }

    /* --------------------------------- MedDRA --------------------------------- */

    @Test
    void meddraSearchShortestFirst() throws Exception {
        call("/v1/meddra?q=re")
                .andExpect(jsonPath("$[0].term").value("Seizure"));
        call("/v1/meddra?q=impot&tty=llt")
                .andExpect(jsonPath("$", hasSize(1)))
                .andExpect(jsonPath("$[0].code").value("10021515"));
        call("/v1/meddra?q=x").andExpect(status().isBadRequest());
    }

    @Test
    void reverseLookupByPtIncludesLlts() throws Exception {
        call("/v1/meddra/10061461/products")
                .andExpect(jsonPath("$.total").value(1))
                .andExpect(jsonPath("$.items[0].productId").value(1))
                .andExpect(jsonPath("$.items[0].meddra.term").value("Impotence"));
    }

    @Test
    void reverseLookupFilters() throws Exception {
        call("/v1/meddra/10019851/products").andExpect(jsonPath("$.total").value(2));
        call("/v1/meddra/10019851/products?boxedWarning=true")
                .andExpect(jsonPath("$.total").value(1))
                .andExpect(jsonPath("$.items[0].productNames", contains("Avodart", "Avodart 0.5 mg")));
        call("/v1/meddra/10004446/products?role=indication")
                .andExpect(jsonPath("$.items[0].productId").value(1))
                .andExpect(jsonPath("$.items[0].warning").doesNotExist());
        call("/v1/meddra/10004446/products?role=bogus").andExpect(status().isBadRequest());
        call("/v1/meddra/abc/products").andExpect(status().isBadRequest());
    }

    /* --------------------------------- Labels --------------------------------- */

    @Test
    void labelMetadata() throws Exception {
        call("/v1/labels/guid-avodart")
                .andExpect(jsonPath("$.productIds", contains(1)))
                .andExpect(jsonPath("$.files[0].xmlFile").value("avodart.xml"));
        call("/v1/labels/missing").andExpect(status().isNotFound());
    }

    @Test
    void labelSectionNewestFirst() throws Exception {
        call("/v1/labels/guid-avodart/sections/adverse-reactions")
                .andExpect(jsonPath("$", hasSize(2)))
                .andExpect(jsonPath("$[0].labelDate").value("2010-06-01"))
                .andExpect(jsonPath("$[0].text", containsString("Impotence")))
                .andExpect(jsonPath("$[0].section").value("ADVERSE_REACTIONS"));
        call("/v1/labels/guid-avodart/sections/boxed-warning").andExpect(status().isNotFound());
        call("/v1/labels/guid-avodart/sections/dosage").andExpect(status().isBadRequest());
    }
}
