package org.apache.bookkeeper.client;

import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
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
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class LedgerHandleAddEntryTest extends BookKeeperClusterTestCase {
    // Test parameters.
    private static byte[] data;
    private static int offset;
    private static int length;
    private static String expectedBehavior;

    // Class under test.
    private LedgerHandle lh;

    /**
     * Inner class containing the test parameters.
     */
    private static class LedgerHandleAddEntryTestParams {
        private byte[] data;
        private int offset;
        private int length;
        private String expectedBehavior;

        public LedgerHandleAddEntryTestParams(byte[] data, int offset, int length, String expectedBehavior) {
            this.data = data;
            this.offset = offset;
            this.length = length;
            this.expectedBehavior = expectedBehavior;
        }
    }

    public LedgerHandleAddEntryTest(LedgerHandleAddEntryTestParams params) {
        super(BOOKIES_NO);
        configure(params);
    }

    /**
     * Link class parameters with test parameters.
     *
     * @param params test parameters
     */
    public void configure(LedgerHandleAddEntryTestParams params) {
        this.data = params.data;
        this.offset = params.offset;
        this.length = params.length;
        this.expectedBehavior = params.expectedBehavior;
    }

    /**
     * Before each test, instantiate the class under test by creating a Ledger.
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
    }

    /**
     * Parameters association. The parameter involved has been declared earlier as
     * attributes in this class.
     *
     * @return array containing actual values of the test parameters
     */
    @Parameterized.Parameters
    public static Collection<LedgerHandleAddEntryTestParams[]> getTestParameters() {
        // Build an array with bytes out of the byte size.
        Integer outOfBounds = -4000;
        Byte outOfBoundsByteVal = outOfBounds.byteValue();
        byte[] outOfBoundsByteArr = {outOfBoundsByteVal, outOfBoundsByteVal};
        int datalen = VALID_STRING.length();
        List<LedgerHandleAddEntryTestParams[]> args = Arrays.asList(new LedgerHandleAddEntryTestParams[][]{
                {new LedgerHandleAddEntryTestParams(VALID_STRING.getBytes(), 1, datalen - 1, PASS)},
                {new LedgerHandleAddEntryTestParams(VALID_STRING.getBytes(), datalen - 1, 0, PASS)},
                {new LedgerHandleAddEntryTestParams(VALID_STRING.getBytes(), datalen, 0, PASS)},
                {new LedgerHandleAddEntryTestParams(VALID_STRING.getBytes(), 0, -1, FAIL)},
                {new LedgerHandleAddEntryTestParams(VALID_STRING.getBytes(), -1, datalen, FAIL)},
                {new LedgerHandleAddEntryTestParams(VALID_STRING.getBytes(), datalen + 1, 1, FAIL)},
                {new LedgerHandleAddEntryTestParams(VALID_STRING.getBytes(), 1, datalen, FAIL)},
                {new LedgerHandleAddEntryTestParams(EMPTY_STRING.getBytes(), 0, datalen, FAIL)},
                {new LedgerHandleAddEntryTestParams(outOfBoundsByteArr, 0, datalen, FAIL)},
                {new LedgerHandleAddEntryTestParams(null, 0, datalen, FAIL)},
        });
        return args;
    }

    @Test
    public void ledgerWriteThenReadTest() {
        long ret = -1;
        try {
            // Try the method under test
            ret = lh.addEntry(data, offset, length);
        } catch (Exception e) {
            assertTrue(buildExceptionString(e), expectedBehavior == FAIL);
            return;
        }
        assertTrue("Not valid entry ID: " + ret, ret >= 0);

        //Now read back, just to verify if what is read is what actually was written
        Enumeration<LedgerEntry> entries = null;
        try {
            entries = lh.readEntries(0, lh.getLastAddConfirmed());
        } catch (Exception e) {
            assertTrue(buildExceptionString(e), expectedBehavior == FAIL);
        }
        while (entries.hasMoreElements()) {
            LedgerEntry entry = entries.nextElement();
            byte[] returned = entry.getEntry();
            String exp = new String(data).substring(offset, offset + length);
            assertTrue("Reading something different than what was sent in input." + exp + " ### " + new String(returned), Arrays.equals(exp.getBytes(), returned));
        }
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
