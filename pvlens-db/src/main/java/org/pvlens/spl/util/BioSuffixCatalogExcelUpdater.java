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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.Map;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

/**
 * Writes a "DerivedSuffixes" sheet into a copy of the FDA Excel file. Safer
 * than in-place edits; reviewers can diff/inspect.
 */
public final class BioSuffixCatalogExcelUpdater {

	private BioSuffixCatalogExcelUpdater() {
	}

	public static Path enrichExcelWithDerivedSuffixes(Path inputXlsx, Map<String, String> suffixToBase,
			Map<String, String> suffixToExample) throws IOException {
		if (!Files.exists(inputXlsx))
			throw new FileNotFoundException(inputXlsx.toString());
		Path outPath = inputXlsx.resolveSibling(stripExt(inputXlsx.getFileName().toString()) + "_enriched.xlsx");

		try (InputStream in = Files.newInputStream(inputXlsx); Workbook wb = new XSSFWorkbook(in)) {

			// Drop existing DerivedSuffixes sheet if present
			int existing = wb.getSheetIndex("DerivedSuffixes");
			if (existing >= 0)
				wb.removeSheetAt(existing);

			Sheet sheet = wb.createSheet("DerivedSuffixes");
			int r = 0;
			Row header = sheet.createRow(r++);
			write(header, 0, "suffix");
			write(header, 1, "base_name");
			write(header, 2, "example_term");
			write(header, 3, "source");
			write(header, 4, "last_updated");

			for (Map.Entry<String, String> e : suffixToBase.entrySet()) {
				String sfx = e.getKey();
				String base = e.getValue();
				String example = suffixToExample.getOrDefault(sfx, "");
				Row row = sheet.createRow(r++);
				write(row, 0, sfx);
				write(row, 1, base);
				write(row, 2, example);
				write(row, 3, "RxNorm SBD");
				write(row, 4, LocalDate.now().toString());
			}

			autosize(sheet, 5);

			try (OutputStream out = Files.newOutputStream(outPath)) {
				wb.write(out);
			}
		}
		return outPath;
	}

	private static void write(Row row, int col, String val) {
		Cell cell = row.createCell(col, CellType.STRING);
		cell.setCellValue(val == null ? "" : val);
	}

	private static void autosize(Sheet sheet, int cols) {
		for (int i = 0; i < cols; i++)
			sheet.autoSizeColumn(i);
	}

	private static String stripExt(String name) {
		int dot = name.lastIndexOf('.');
		return (dot > 0) ? name.substring(0, dot) : name;
	}
}
