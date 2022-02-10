package org.apache.bookkeeper.util;

import org.apache.bookkeeper.client.BookKeeper;

/**
 * Utility class for global string values, passing expectations statements,
 * and other specific tools, used (statically) in the "2" Apache BookKeeper project.
 */
public final class ISW2TestUtils {
    // Sample string values.
    public static final String VALID_STRING = "This is, actually, a valid string";
    public static final String EMPTY_STRING = "";
    public static final String PASSWORD = "password";

    // Sample valid byte buffer, and relative parameters.
    public static byte[] validData = VALID_STRING.getBytes();
    public static final int validOffset = 0;
    public static int validLength = validData.length;

    // Passing expectations. Only high-level "pass/fail" logic, based on a code-agnostic approach.
    public static final String PASS = "Success.";
    public static final String FAIL = "Fail.";
    public static final String EXPECTED_TO_FAIL = "Test meant to fail, but did not.";

    /**
     * Custom exception string.
     *
     * @see ISW2TestUtils :buildExceptionString
     */
    public static final String NOT_EXPECTED_EXCEPTION = "Exception raised, but not expected here: ";

    // BookKeeper-specific default parameters used in the tests.
    public static final int BOOKIES_NO = 5;
    public static final BookKeeper.DigestType DIGEST_TYPE_DEFAULT = BookKeeper.DigestType.CRC32;
    public static final int ENTRIES_NO = 10;
    public static final String TESTFILE_PATHNAME = "/tmp/bookkeeper/test";
    public static final int BYTEBUFFER_DIM = 40;

    /**
     * Default constructor, to prevent instatiation
     */
    private ISW2TestUtils() {
    }

    /**
     * Build a specific error string, to be made distinct w.r.t. the standard strings not included in the tests.
     *
     * @param e the exception raised
     * @return the built string
     */
    public static String buildExceptionString(Exception e) {
        return NOT_EXPECTED_EXCEPTION + e.toString();
    }
}
