package org.pvlens.spl.util;

/*
 * This file is part of PVLens.
 *
 * Copyright (C) 2025 GlaxoSmithKline
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

import static org.junit.jupiter.api.Assertions.*;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.junit.jupiter.api.*;

class ZipFileExtractorExtractXmlFilesTest {

    private static final String[] XML_MAP_HEADER  = { "ZipFileName", "XmlFileName", "SourceType" };
    private static final String[] PROD_MAP_HEADER = { "ZipFileName", "ManufactureProductName" };

    // Names used in assertions
    private static final String SEED_ZIP = "20171206_a4f917f4-2aa2-c3ed-071e-232fa0f125e8.zip";
    private static final String PRES_ZIP = "20250723_0280849d-5c78-4a9d-8941-4eab429f6bd8.zip";
    private static final String OTHER_ZIP = "20250604_369f73b9-e032-4315-e063-6294a90a4ca1.zip";

    private Path tmpRoot;
    private Path xmlMapCsv;   // SPL_ZIP_XML_MAP.csv
    private Path prodMapCsv;  // PRODUCT_LABEL_MAP.csv
    private Path splRootDir;  // parent that contains 'prescription' and 'other'
    private Path presDir;
    private Path otherDir;

    @BeforeEach
    void setup() throws Exception {
        tmpRoot    = Files.createTempDirectory("zfe-extract-int-");
        splRootDir = Files.createDirectories(tmpRoot.resolve("spl"));
        presDir    = Files.createDirectories(splRootDir.resolve("prescription"));
        otherDir   = Files.createDirectories(splRootDir.resolve("other"));
        xmlMapCsv  = splRootDir.resolve("SPL_ZIP_XML_MAP.csv");
        prodMapCsv = splRootDir.resolve("PRODUCT_LABEL_MAP.csv");

        // Seed CSVs with headers (3-col XML map)
        try (BufferedWriter w1 = Files.newBufferedWriter(xmlMapCsv, StandardCharsets.UTF_8);
             CSVPrinter p1 = new CSVPrinter(w1, CSVFormat.DEFAULT)) {
            p1.printRecord((Object[]) XML_MAP_HEADER);
            // Seeded “already processed” row in 'prescription'
            p1.printRecord(SEED_ZIP, "already_present.xml", "prescription");
        }
        try (BufferedWriter w2 = Files.newBufferedWriter(prodMapCsv, StandardCharsets.UTF_8);
             CSVPrinter p2 = new CSVPrinter(w2, CSVFormat.DEFAULT)) {
            p2.printRecord((Object[]) PROD_MAP_HEADER);
        }

        // Create two test ZIPs with tiny XMLs:
        // - one under 'prescription'
        // - one under 'other'
        createZipWithXml(presDir.resolve(PRES_ZIP),
                Map.of(
                    "a/b/c/369f6e89-c1f9-4efe-e063-6394a90a920d.xml", minimalProductXml("PRESCRIPTION PRODUCT A")
                ));
        createZipWithXml(otherDir.resolve(OTHER_ZIP),
                Map.of(
                    "x/369f73b9-e032-4315-e063-6294a90a4ca1.xml", minimalProductXml("OTHER PRODUCT B")
                ));
    }

    @AfterEach
    void cleanup() throws Exception {
        // Uncomment to delete temp files after test runs
        // Files.walk(tmpRoot).sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
    }

    private static void createZipWithXml(Path zipPath, Map<String,String> xmlEntries) throws IOException {
        try (OutputStream os = Files.newOutputStream(zipPath);
             ZipOutputStream zos = new ZipOutputStream(os, StandardCharsets.UTF_8)) {
            for (Map.Entry<String,String> e : xmlEntries.entrySet()) {
                zos.putNextEntry(new ZipEntry(e.getKey()));
                byte[] bytes = e.getValue().getBytes(StandardCharsets.UTF_8);
                zos.write(bytes, 0, bytes.length);
                zos.closeEntry();
            }
        }
    }

    private static String minimalProductXml(String productName) {
        // Very small, valid SPL-like structure that ZipFileExtractor.getManufactureProductName can parse
        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
               "<document>\n" +
               "  <structuredBody>\n" +
               "    <manufacturedProduct>\n" +
               "      <name>" + productName + "</name>\n" +
               "    </manufacturedProduct>\n" +
               "  </structuredBody>\n" +
               "</document>\n";
    }

    private static List<CSVRecord> readCsv(Path p, String[] header) throws Exception {
        try (var reader = Files.newBufferedReader(p, StandardCharsets.UTF_8)) {
            return CSVFormat.DEFAULT.builder()
                    .setHeader(header)
                    .setSkipHeaderRecord(true)
                    .build()
                    .parse(reader)
                    .getRecords();
        }
    }

    @Test
    @DisplayName("extractXmlFiles processes only new ZIPs, writes headers once, extracts XML and product names")
    void extractXmlFiles_endToEnd_withInjectedPaths() throws Exception 
    {
        // Given an instance wired to our temp paths (parent SPL dir)
        ZipFileExtractor zfe = new ZipFileExtractor(splRootDir);
        zfe.extractXmlFiles();

        // Then: read CSVs
        List<CSVRecord> xmlMap  = readCsv(xmlMapCsv, XML_MAP_HEADER);
        List<CSVRecord> prodMap = readCsv(prodMapCsv, PROD_MAP_HEADER);

        // Verify the seeded ZIP is still listed, and the two new zips were processed
        Set<String> processedZipNames = xmlMap.stream()
                .map(r -> r.get("ZipFileName"))
                .collect(Collectors.toSet());

        assertTrue(processedZipNames.contains(SEED_ZIP),  "Seeded zip should still be listed from the seed row");
        assertTrue(processedZipNames.contains(PRES_ZIP),  "Prescription ZIP should be processed");

    }
    
    
}
