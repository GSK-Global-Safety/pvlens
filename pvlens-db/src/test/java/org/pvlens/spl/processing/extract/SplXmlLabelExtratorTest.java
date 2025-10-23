package org.pvlens.spl.processing.extract;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

import org.junit.jupiter.api.Test;

public class SplXmlLabelExtratorTest {

	@Test
	void testAeExtractionForSQL() {

		// Actual text from OTC
		// found in a51fe7c6-e55c-4be3-a829-0f4d01557854.xml
		String sampleText = """
				              DISTRIBUTED BY CORP, LLC
				              100 MAIN ST,
				              CITY, NC 12345
				              100% QUALITY GUARANTEED (888)555-1234\\""";
		
		
		// The output should be the same as the input in this case
		String sqlSafe = SplXmlLabelExtractor.sanitizeForSQLPlainLiteral(sampleText);
		String expOutput = """
				DISTRIBUTED BY CORP, LLC
				100 MAIN ST,
				CITY, NC 12345
				100% QUALITY GUARANTEED (888)555-1234
				""";
		assertTrue(sqlSafe.contentEquals(expOutput.trim()));
		
	}
	
	  // Convenience alias
    private static String S(String s) {
        return SplXmlLabelExtractor.sanitizeForSQLPlainLiteral(s);
    }

    @Test
    void preservesNewlinesAndEscapesSingleQuotes() {
        String in = "O'Brien\nSecond line";
        String out = S(in);
        assertEquals("O''Brien\nSecond line", out);
    }

    @Test
    void collapsesOverEscapedFourQuotesToTwo() {
        // Simulate double-sanitized interior: O''''Brien -> should be O''Brien
        String in = "O''''Brien";
        String out = S(in);
        assertEquals("O''Brien", out);
    }

    @Test
    void stripsControlCharsButKeepsLfCrTab() {
        String in = "line\u0000one\nline\u0001two\rline\tthree";
        String out = S(in);
        // Note: standalone \r is normalized to \n now
        assertEquals("lineone\nlinetwo\nline\tthree", out);
        assertFalse(out.contains("\u0000"));
        assertFalse(out.contains("\u0001"));
    }

    @Test
    void normalizesCrLfAndCrToLf() {

        String in = "a\r\nb\rc";
        String out = S(in);
        assertEquals("a\nb\nc", out);
    }

    @Test
    void doublesBackslashBeforeNewlineOrEosButNotElsewhere() {
        String in = "path\\to\\file\nendsWithSlash\\";
        String out = S(in);
        // before newline doubled, interior backslash left as-is, final run of backslashes removed by our rule
        assertEquals("path\\to\\file\nendsWithSlash", out);
    }

    @Test
    void removesAllTrailingBackslashes() {
        assertEquals("abc", S("abc\\"));
        assertEquals("abc", S("abc\\\\"));
        assertEquals("abc", S("abc\\\\\\"));
    }

    @Test
    void neverEndsWithOrphanSingleQuote() {
        String in = "abc'"; // lone '
        String out = S(in);
        assertEquals("abc", out);
        // already doubled at end is okay and preserved
        assertEquals("abc''", S("abc''"));
    }

    @Test
    void avoidsDanglingHighSurrogateAtEnd() {
        // U+D83D is a high surrogate for many emoji; as a dangling tail it should be dropped
        String in = "emoji start \uD83D";
        String out = S(in);
        assertEquals("emoji start ", out);
        // Full surrogate pair should be preserved
        String ok = "smile \uD83D\uDE03"; // 😃
        assertEquals(ok, S(ok));
    }

    @Test
    void idempotentOnAlreadySanitizedText() {
        String once = S("O'Brien \\ end\\");
        String twice = S(once);
        assertEquals(once, twice, "Sanitization should be idempotent");
    }

    @Test
    void truncationDoesNotSplitDoubledQuoteOrLeaveBackslash() {
        // Build a string that ends right on a single quote after truncation, and one that ends with backslash
        String base = "X".repeat(15795); // leave space for tail
        String withQuoteTail = base + "abc'";   // would end on orphan '
        String withSlashTail = base + "abc\\";  // would end on single backslash

        String outQuote = S(withQuoteTail);
        assertFalse(outQuote.endsWith("'"), "Should not end with a lone single quote");

        String outSlash = S(withSlashTail);
        assertFalse(outSlash.endsWith("\\"), "Should not end with a single backslash");
    }

    @Test
    void handlesMixedComplexCase() {
        String in = "Line1: O'Brien\r\nLine2: path\\tmp\\\nLine3: ctrl\u0000x ''''\nEnd\\";
        String out = S(in);
        // Expectations:
        // - CRLF -> LF; ctrl removed; four quotes -> two; trailing slashes removed
        String expected =
            "Line1: O''Brien\n" +
            "Line2: path\\tmp\\\n" +   // backslash before newline doubled then final trailing run completely removed at very end only
            "Line3: ctrlx ''\n" +
            "End";
        assertEquals(expected, out);
    }

    @Test
    void nullAndEmptyAreSafe() {
        assertEquals("", S(null));
        assertEquals("", S(""));
    }

}
