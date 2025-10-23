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
import java.io.InputStream;
import java.lang.reflect.Method;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.junit.jupiter.api.*;

class ZipFileExtractorIntegrationTest {

    // CSV headers the production class uses
    private static final String[] XML_MAP_HEADER  = { "ZipFileName", "XmlFileName" };
    private static final String[] PROD_MAP_HEADER = { "ZipFileName", "ManufactureProductName" };

    private Path tmpDir;
    private Path outXmlDir;
    private Path outCsvDir;
    private Path xmlMapCsv;
    private Path prodMapCsv;

    private CSVPrinter xmlMapPrinter;
    private CSVPrinter prodMapPrinter;

    @BeforeEach
    void setup() throws Exception {
        tmpDir    = Files.createTempDirectory("zfe-inttest-");
        outXmlDir = Files.createDirectory(tmpDir.resolve("xml-out"));
        outCsvDir = Files.createDirectory(tmpDir.resolve("csv-out"));

        xmlMapCsv = outCsvDir.resolve("SPL_ZIP_XML_MAP.csv");
        prodMapCsv = outCsvDir.resolve("PRODUCT_LABEL_MAP.csv");

        // create printers with headers (mimic production append behavior)
        BufferedWriter w1 = Files.newBufferedWriter(xmlMapCsv);
        xmlMapPrinter = new CSVPrinter(w1, CSVFormat.DEFAULT);
        xmlMapPrinter.printRecord((Object[]) XML_MAP_HEADER);
        xmlMapPrinter.flush();

        BufferedWriter w2 = Files.newBufferedWriter(prodMapCsv);
        prodMapPrinter = new CSVPrinter(w2, CSVFormat.DEFAULT);
        prodMapPrinter.printRecord((Object[]) PROD_MAP_HEADER);
        prodMapPrinter.flush();
    }

    @AfterEach
    void cleanup() throws Exception {
        if (xmlMapPrinter != null) xmlMapPrinter.close();
        if (prodMapPrinter != null) prodMapPrinter.close();
        // Leave tmpDir for debugging if you want; otherwise:
        // Files.walk(tmpDir).sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
    }

    /** Reflectively invoke the private processZipFile(Path, Path, CSVPrinter, CSVPrinter). */
    private void processZip(Path zip) throws Exception {
        ZipFileExtractor zfe = new ZipFileExtractor("");
        Method m = ZipFileExtractor.class.getDeclaredMethod(
                "processZipFile", Path.class, Path.class, CSVPrinter.class, CSVPrinter.class);
        m.setAccessible(true);
        m.invoke(zfe, zip, outXmlDir, xmlMapPrinter, prodMapPrinter);
        xmlMapPrinter.flush();
        prodMapPrinter.flush();
    }

    /** Copy a resource from classpath to temp dir and return its Path. */
    private Path copyResourceToTemp(String resourcePath, String outName) throws Exception {
        try (InputStream is = getClass().getResourceAsStream(resourcePath)) {
            assertNotNull(is, "Missing test resource: " + resourcePath);
            Path dst = tmpDir.resolve(outName);
            Files.copy(is, dst, StandardCopyOption.REPLACE_EXISTING);
            return dst;
        }
    }

    private List<CSVRecord> readCsv(Path p, String[] header) throws Exception {
        try (var reader = Files.newBufferedReader(p)) {
            return CSVFormat.DEFAULT.builder()
                    .setHeader(header)
                    .setSkipHeaderRecord(true)
                    .build()
                    .parse(reader)
                    .getRecords();
        }
    }

    @Test
    @DisplayName("Process three real SPL ZIPs and verify CSV maps + extracted product names")
    void processRealZips() throws Exception {
        // Arrange: copy the three real test ZIPs to temp
        Path z1 = copyResourceToTemp("/spl/zip/20171206_a4f917f4-2aa2-c3ed-071e-232fa0f125e8.zip",
                                     "20171206_a4f917f4-2aa2-c3ed-071e-232fa0f125e8.zip");
        Path z2 = copyResourceToTemp("/spl/zip/20250723_0280849d-5c78-4a9d-8941-4eab429f6bd8.zip",
                                     "20250723_0280849d-5c78-4a9d-8941-4eab429f6bd8.zip");
        Path z3 = copyResourceToTemp("/spl/zip/20250604_369f73b9-e032-4315-e063-6294a90a4ca1.zip",
                                     "20250604_369f73b9-e032-4315-e063-6294a90a4ca1.zip");

        // Act: process each with the private method
        processZip(z1);
        processZip(z2);
        processZip(z3);

        // Assert: CSV contents
        List<CSVRecord> xmlMap = readCsv(xmlMapCsv, XML_MAP_HEADER);
        List<CSVRecord> prodMap = readCsv(prodMapCsv, PROD_MAP_HEADER);

        // Build a map of zip -> set of xmls
        Map<String, Set<String>> zipToXmls = xmlMap.stream()
            .collect(Collectors.groupingBy(
                r -> r.get(XML_MAP_HEADER[0]),
                Collectors.mapping(r -> r.get(XML_MAP_HEADER[1]), Collectors.toSet())
            ));

        // Build a multimap of zip -> product names
        Map<String, Set<String>> zipToProds = prodMap.stream()
            .collect(Collectors.groupingBy(
                r -> r.get(PROD_MAP_HEADER[0]),
                Collectors.mapping(r -> r.get(PROD_MAP_HEADER[1]), Collectors.toSet())
            ));

        // We expect exactly one XML per these sample zips (based on your fixture list)
        assertTrue(zipToXmls.containsKey(z1.getFileName().toString()));
        assertTrue(zipToXmls.containsKey(z2.getFileName().toString()));
        assertTrue(zipToXmls.containsKey(z3.getFileName().toString()));

        // Extracted product names exist
        assertTrue(zipToProds.get(z1.getFileName().toString()).contains("PRIMIDONE"));
        assertTrue(zipToProds.get(z2.getFileName().toString()).contains("SHINGRIX"));
        assertTrue(zipToProds.get(z3.getFileName().toString()).contains("DUTASTERIDE"));

        // Also assert the XML files were written to outXmlDir and can be parsed for name again
        for (String zipName : zipToXmls.keySet()) {
            for (String xmlRel : zipToXmls.get(zipName)) {
                Path xmlPath = outXmlDir.resolve(xmlRel.replace('\\', '/')).normalize();
                assertTrue(Files.exists(xmlPath), "Extracted XML not found: " + xmlPath);
            }
        }
    }
}
