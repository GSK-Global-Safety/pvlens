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

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;

/** OpenAPI document served at /v3/api-docs and /swagger-ui.html. */
@Configuration
public class OpenApiConfig {

    @Bean
    OpenAPI pvlensOpenApi() {
        return new OpenAPI()
                .info(new Info()
                        .title("PVLens API")
                        .version("v1")
                        .description("Read-only access to drug label safety data extracted by PVLens: "
                                + "products, labeled adverse events and indications mapped to MedDRA, "
                                + "label text and FDA safety related label changes. "
                                + "Contains MedDRA and UMLS content; use is subject to those licenses.")
                        .license(new License().name("GPL-3.0").url("https://www.gnu.org/licenses/gpl-3.0.html")))
                .components(new Components().addSecuritySchemes("apiKey", new SecurityScheme()
                        .type(SecurityScheme.Type.APIKEY)
                        .in(SecurityScheme.In.HEADER)
                        .name(ApiKeyFilter.HEADER)))
                .addSecurityItem(new SecurityRequirement().addList("apiKey"));
    }
}
