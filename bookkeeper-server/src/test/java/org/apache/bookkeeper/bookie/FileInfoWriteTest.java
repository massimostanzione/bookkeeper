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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.apache.bookkeeper.util.ISW2TestUtils.*;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class FileInfoWriteTest extends BookKeeperClusterTestCase {
    // Test parameters.
    private static ByteBuffer[] buffArray;
    private static long position;
    private static String expectedBehavior;

    private File f;

    // Class under test.
    private FileInfo fInfo;

    /**
     * Inner class containing the test parameters.
     */
    private static class FileInfoWriteTestParams {
        private ByteBuffer[] buffArray;
        private long position;
        private String expectedBehavior;

        public FileInfoWriteTestParams(ByteBuffer[] buffArray, long position, String expectedBehavior) {
            this.buffArray = buffArray;
            this.position = position;
            this.expectedBehavior = expectedBehavior;
        }
    }

    public FileInfoWriteTest(FileInfoWriteTestParams params) {
        super(BOOKIES_NO);
        configure(params);
    }

    /**
     * Link class parameters with test parameters.
     *
     * @param params test parameters
     */
    public void configure(FileInfoWriteTestParams params) {
        this.buffArray = params.buffArray;
        this.position = params.position;
        this.expectedBehavior = params.expectedBehavior;
    }

    /**
     * Before each test, instantiate the class under test by creating a file
     * and assigning it to the FileInfo.
     * Exceptions not specifically handled because not under test.
     */
    @Before
    public void setup() {
        f = new File(TESTFILE_PATHNAME);
        /* Attempt to deal with short writes
        try {
            f.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
        RandomAccessFile RAf;
        try {
            RAf = new RandomAccessFile(f, "rw");
            RAf.setLength(1);
        } catch (Exception e) {
            e.printStackTrace();
        }*/
        try {
            fInfo = new FileInfo(f, PASSWORD.getBytes());
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
    public static Collection<FileInfoWriteTestParams[]> getTestParameters() {
        ByteBuffer[] notPopulatedByteBuf = new ByteBuffer[BYTEBUFFER_DIM];
        ByteBuffer[] outOfBoundsByteBuf = new ByteBuffer[BYTEBUFFER_DIM];

        // Build an array with bytes out of the byte size.
        Integer outOfBounds = -4000;
        byte outOfBoundsByteVal = outOfBounds.byteValue();
        byte outOfBoundsByteArr[] = {outOfBoundsByteVal, outOfBoundsByteVal};
        for (int i = 0; i < BYTEBUFFER_DIM; i++) {
            outOfBoundsByteBuf[i] = ByteBuffer.wrap(outOfBoundsByteArr);
        }
        List<FileInfoWriteTestParams[]> args = Arrays.asList(new FileInfoWriteTestParams[][]{
                {new FileInfoWriteTestParams(getValidBufferArray(), -1, PASS)},
                {new FileInfoWriteTestParams(getValidBufferArray(), 0, PASS)},
                {new FileInfoWriteTestParams(getValidBufferArray(), 1, PASS)},
                {new FileInfoWriteTestParams(notPopulatedByteBuf, 0, FAIL)},
                {new FileInfoWriteTestParams(outOfBoundsByteBuf, 0, PASS)},
                {new FileInfoWriteTestParams(null, 0, FAIL)},
        });
        return args;
    }

    /**
     * Utility method to provide a "valid" array, implemented because of
     * the "re-writability" of the ByteBuffers when called in the test.
     *
     * @return "valid" (populated) array
     */
    private static ByteBuffer[] getValidBufferArray() {
        // Fill in the valid ByteBuffer
        ByteBuffer[] validByteBuf = new ByteBuffer[BYTEBUFFER_DIM];
        for (int i = 0; i < BYTEBUFFER_DIM; i++) {
            validByteBuf[i] = ByteBuffer.wrap((VALID_STRING.getBytes()));
        }
        return validByteBuf;
    }

    @Test
    public void fileInfoWriteThenReadTest() {
        long writtenBytesNo = 0, buffArraySize = 0;
        int readBytesNo = 0;
        try {
            writtenBytesNo = fInfo.write(buffArray, position);
        } catch (Exception e) {
            assertTrue(buildExceptionString(e), expectedBehavior == FAIL);
            return;
        }
        for (int i = 0; i < buffArray.length; i++) {
            if (buffArray[i] != null) {
                buffArraySize += buffArray[i].position();
            }
        }
        assertTrue("Invalid number of written bytes.", writtenBytesNo >= 0);
        assertTrue("Written a different amount of bytes than expected. " + buffArraySize + " vs " + writtenBytesNo, (buffArraySize == writtenBytesNo && expectedBehavior == PASS)
                || (buffArraySize != writtenBytesNo && expectedBehavior == FAIL));
        // If the previous asserts were correct and correctly failed, the test is to be considered as failed.
        if (expectedBehavior == FAIL) return;

        // Now read back
        ByteBuffer readBuf = ByteBuffer.allocate((int) buffArraySize);
        try {
            readBytesNo = fInfo.read(readBuf, position, true);
        } catch (IOException e) {
            // Not managed - it is not in the purpose of the test
            e.printStackTrace();
        }
        assertTrue("Read a different amount of bytes than expected. " + readBytesNo + " vs " + writtenBytesNo, (readBytesNo == writtenBytesNo && expectedBehavior == PASS)
                || (readBytesNo != writtenBytesNo && expectedBehavior == FAIL));
        assertTrue(EXPECTED_TO_FAIL, expectedBehavior == PASS);
    }

    /**
     * Clean up the environment, at the ond of each test.
     */
    @After
    public void teardown() {
        try {
            fInfo.close(true);
        } catch (IOException e) {
            e.printStackTrace();
        }
        fInfo.release();
        fInfo.delete();
        f.delete();
    }
}
