package org.apache.bookkeeper.client;

import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.commons.lang.ArrayUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;

import static org.apache.bookkeeper.util.ISW2TestUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class LedgerHandleReadEntriesTest extends BookKeeperClusterTestCase {
    // Test parameters.
    private static long first;
    private static long last;
    private static String expectedBehavior;

    // Class under test.
    private LedgerHandle lh;

    /**
     * Inner class containing the test parameters.
     */
    private static class LedgerHandleReadEntriesTestParams {
        private long first;
        private long last;
        private String expectedBehavior;

        public LedgerHandleReadEntriesTestParams(long first, long last, String expectedBehavior) {
            this.first = first;
            this.last = last;
            this.expectedBehavior = expectedBehavior;
        }
    }

    public LedgerHandleReadEntriesTest(LedgerHandleReadEntriesTestParams params) {
        super(BOOKIES_NO);
        configure(params);
    }

    /**
     * Link class parameters with test parameters.
     *
     * @param params test parameters
     */
    public void configure(LedgerHandleReadEntriesTestParams params) {
        this.first = params.first;
        this.last = params.last;
        this.expectedBehavior = params.expectedBehavior;
    }

    /**
     * Before each test, instantiate the class under test by creating a Ledger
     * and writing ENTRIES_NO entries into it.
     * Exceptions not specifically handled because not under test.
     */
    @Before
    public void setup() {
        try {
            lh = bkc.createLedger(DIGEST_TYPE_DEFAULT, PASSWORD.getBytes());
        } catch (BKException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        for (int i = 0; i < ENTRIES_NO; i++) {
            try {
                lh.addEntry(validData, validOffset, validLength);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (BKException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Parameters association. The parameter involved has been declared earlier as
     * attributes in this class.
     *
     * @return array containing actual values of the test parameters
     */
    @Parameterized.Parameters
    public static Collection<LedgerHandleReadEntriesTestParams[]> getTestParameters() {
        List<LedgerHandleReadEntriesTestParams[]> args = Arrays.asList(new LedgerHandleReadEntriesTestParams[][]{
                {new LedgerHandleReadEntriesTestParams(1, 2, PASS)},
                {new LedgerHandleReadEntriesTestParams(1, 1, PASS)},
                {new LedgerHandleReadEntriesTestParams(0, -1, FAIL)},
                {new LedgerHandleReadEntriesTestParams(-1, 0, FAIL)},
                // enhance coverace (adequacy)
                {new LedgerHandleReadEntriesTestParams(0, ENTRIES_NO - 1, PASS)},
                {new LedgerHandleReadEntriesTestParams(0, ENTRIES_NO, FAIL)},
                {new LedgerHandleReadEntriesTestParams(0, ENTRIES_NO + 1, FAIL)},
        });
        return args;
    }

    @Test
    public void ledgerReadEntriesTest() {
        Enumeration<LedgerEntry> entries = null;
        try {
            // Try the method under test
            entries = lh.readEntries(first, last);
        } catch (Exception e) {
            assertTrue(buildExceptionString(e), expectedBehavior == FAIL);
            return;
        }
        assertTrue("Enumeration read is empty.", entries.hasMoreElements());
        int readEntries = 0;
        // Read the single entries and check if what is read is what was written.
        while (entries.hasMoreElements()) {
            LedgerEntry entry = entries.nextElement();
            assertTrue("Reading something different than what was sent in input.", Arrays.equals(entry.getEntry(), ArrayUtils.subarray(validData, validOffset, validLength)));
            readEntries++;
        }
        assertEquals("Reading less entries than how much was sent in input.", last - first + 1, readEntries);
        assertTrue(EXPECTED_TO_FAIL, expectedBehavior == PASS);
    }

    /**
     * Clean up the environment, at the ond of each test.
     */
    @After
    public void teardown() {
        try {
            lh.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BKException e) {
            e.printStackTrace();
        }
    }
}
