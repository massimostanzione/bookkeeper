package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.apache.bookkeeper.util.ISW2TestUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class FileInfoReadTest extends BookKeeperClusterTestCase {
    // Test parameters.
    private static ByteBuffer buff;
    private static long position;
    private static boolean bestEffort;
    private static String expectedBehavior;

    private File f;

    // Class under test.
    private FileInfo fInfo;

    /**
     * Inner class containing the test parameters.
     */
    private static class FileInfoReadTestParams {
        private ByteBuffer buff;
        private long position;
        private boolean bestEffort;
        private String expectedBehavior;

        public FileInfoReadTestParams(ByteBuffer buff, long position, boolean bestEffort, String expectedBehavior) {
            this.buff = buff;
            this.position = position;
            this.bestEffort = bestEffort;
            this.expectedBehavior = expectedBehavior;
        }
    }

    public FileInfoReadTest(FileInfoReadTestParams params) {
        super(BOOKIES_NO);
        configure(params);
    }

    /**
     * Link class parameters with test parameters.
     *
     * @param params test parameters
     */
    public void configure(FileInfoReadTestParams params) {
        this.buff = params.buff;
        this.position = params.position;
        this.bestEffort = params.bestEffort;
        this.expectedBehavior = params.expectedBehavior;
    }

    /**
     * Before each test, instantiate the class under test by creating a file,
     * and assigning it to the FileInfo and writing into it.
     * Exceptions not specifically handled because not under test.
     */
    @Before
    public void setup() {
        f = new File(TESTFILE_PATHNAME);
        try {
            fInfo = new FileInfo(f, PASSWORD.getBytes());
        } catch (IOException e) {
            // Not handled, because not object of this test
            e.printStackTrace();
        }
        ByteBuffer[] toWrite = new ByteBuffer[1];
        toWrite[0] = ByteBuffer.wrap(VALID_STRING.getBytes());
        try {
            // Not tested, because it is not under test
            fInfo.write(toWrite, 0);
        } catch (IOException e) {
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
    public static Collection<FileInfoReadTestParams[]> getTestParameters() {
        List<FileInfoReadTestParams[]> args = Arrays.asList(new FileInfoReadTestParams[][]{
                {new FileInfoReadTestParams(getValidBufferArray(), -1, true, PASS)},
                {new FileInfoReadTestParams(getValidBufferArray(), 0, true, PASS)},
                {new FileInfoReadTestParams(getValidBufferArray(), 1, true, PASS)},
                {new FileInfoReadTestParams(getValidBufferArray(), 1, false, FAIL)},
                {new FileInfoReadTestParams(null, 0, true, FAIL)},
        });
        return args;
    }

    /**
     * Utility method to provide an empty array, implemented because of
     * the "re-writability" of the ByteBuffers when called in the test.
     *
     * @return "valid" (allocated) array
     */
    private static ByteBuffer getValidBufferArray() {
        ByteBuffer validBuf = ByteBuffer.allocate(VALID_STRING.length());//TODO const
        return validBuf;
    }

    @Test
    public void fileInfoWriteThenReadTest() {
        /* Attempt to enhance coverage
        try {
            fInfo.close(true);
        } catch (IOException e) {
            e.printStackTrace();
        } */
        int readBytesNo = 0;
        long exp = (VALID_STRING.length() - position);
        try {
            readBytesNo = fInfo.read(buff, position, bestEffort);
        } catch (Exception e) {
            assertTrue(buildExceptionString(e), expectedBehavior == FAIL);
            return;
        }
        assertTrue("Invalid number of read bytes.", readBytesNo >= 0);
        assertTrue("Read a different amount of bytes than expected. Expected TODO" + ", read " + readBytesNo, (readBytesNo == (position < 0 ? exp + position : exp) && expectedBehavior == PASS)
                || (readBytesNo != (position < 0 ? exp + position : exp) && expectedBehavior == FAIL));
        // If the previous asserts were correct and correctly failed, the test is to be considered as failed.
        // Also, to avoid not-relevant exceptions caused by indexes where not needed
        if (expectedBehavior == FAIL) return;
        // Trimming excluded chars, still present into the string, before doing the assertion
        if (position >= 0)
            assertEquals("Reading something different than what was written.", VALID_STRING.substring((int) position, (int) exp), new String(buff.array(), Charset.defaultCharset()).substring(0, (int) (exp - position)));
        assertEquals("Read bytes different than buffer's bytes", readBytesNo, buff.position());
        assertTrue(EXPECTED_TO_FAIL, expectedBehavior == PASS);
    }

    /**
     * Clean up the environment, at the ond of each test.
     */
    @After
    public void teardown() {
        if (buff != null)
            buff.clear();
        try {
            fInfo.flushHeader();
            fInfo.close(true);
        } catch (IOException e) {
            e.printStackTrace();
        }
        fInfo.delete();
    }
}
