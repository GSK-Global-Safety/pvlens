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

import org.springframework.boot.SpringApplication;

/**
 * Runs the API on the in-memory sample database used by the tests, so you can
 * try it without a PVLens MySQL build. Start it with {@code mvnw spring-boot:test-run}
 * and use the API key {@code test-key-1}.
 */
public class DemoPvlensApiApplication {

    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(PvlensApiApplication.class);
        app.setAdditionalProfiles("test");
        app.run(args);
    }
}
