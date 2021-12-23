/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.bookkeeper.bookie;

<<<<<<< HEAD
=======
import static org.junit.Assert.assertEquals;

>>>>>>> 2346686c3b8621a585ad678926adf60206227367
import java.util.Enumeration;

import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
<<<<<<< HEAD
import org.junit.Assert;
import org.junit.Test;

=======
import org.junit.Test;

/**
 * Test the bookie journal without sync.
 */
>>>>>>> 2346686c3b8621a585ad678926adf60206227367
public class BookieJournalNoSyncTest extends BookKeeperClusterTestCase {

    public BookieJournalNoSyncTest() {
        super(1);

        baseConf.setJournalSyncData(false);
    }

    @Test
    public void testWriteToJournal() throws Exception {
        LedgerHandle lh = bkc.createLedger(1, 1, DigestType.CRC32, new byte[0]);

<<<<<<< HEAD
        int N = 10;

        long ledgerId = lh.getId();

        for (int i = 0; i < N; i++) {
=======
        int n = 10;

        long ledgerId = lh.getId();

        for (int i = 0; i < n; i++) {
>>>>>>> 2346686c3b8621a585ad678926adf60206227367
            lh.addEntry(("entry-" + i).getBytes());
        }

        restartBookies();

        LedgerHandle readLh = bkc.openLedger(ledgerId, DigestType.CRC32, new byte[0]);

<<<<<<< HEAD
        Enumeration<LedgerEntry> entries = readLh.readEntries(0, N - 1);
        for (int i = 0; i < N; i++) {
            LedgerEntry entry = entries.nextElement();
            Assert.assertEquals("entry-" + i, new String(entry.getEntry()));
=======
        Enumeration<LedgerEntry> entries = readLh.readEntries(0, n - 1);
        for (int i = 0; i < n; i++) {
            LedgerEntry entry = entries.nextElement();
            assertEquals("entry-" + i, new String(entry.getEntry()));
>>>>>>> 2346686c3b8621a585ad678926adf60206227367
        }
    }

}
