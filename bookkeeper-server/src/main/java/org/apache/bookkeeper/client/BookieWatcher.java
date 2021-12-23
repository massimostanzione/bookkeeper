/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.client;

import java.util.List;
import java.util.Map;
import java.util.Set;
<<<<<<< HEAD
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException.BKInterruptedException;
=======

>>>>>>> 2346686c3b8621a585ad678926adf60206227367
import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieAddressResolver;

/**
 * Watch for Bookkeeper cluster status.
 */
<<<<<<< HEAD
@Slf4j
class BookieWatcher {

    private static final Function<Throwable, BKException> EXCEPTION_FUNC = cause -> {
        if (cause instanceof BKException) {
            log.error("Failed to get bookie list : ", cause);
            return (BKException) cause;
        } else if (cause instanceof InterruptedException) {
            log.error("Interrupted reading bookie list : ", cause);
            return new BKInterruptedException();
        } else {
            return new MetaStoreException();
        }
    };

    private final ClientConfiguration conf;
    private final RegistrationClient registrationClient;
    private final EnsemblePlacementPolicy placementPolicy;

    // Bookies that will not be preferred to be chosen in a new ensemble
    final Cache<BookieSocketAddress, Boolean> quarantinedBookies;

    private volatile Set<BookieSocketAddress> writableBookies = Collections.emptySet();
    private volatile Set<BookieSocketAddress> readOnlyBookies = Collections.emptySet();

    private CompletableFuture<?> initialWritableBookiesFuture = null;
    private CompletableFuture<?> initialReadonlyBookiesFuture = null;

    public BookieWatcher(ClientConfiguration conf,
                         EnsemblePlacementPolicy placementPolicy,
                         RegistrationClient registrationClient) {
        this.conf = conf;
        this.placementPolicy = placementPolicy;
        this.registrationClient = registrationClient;
        this.quarantinedBookies = CacheBuilder.newBuilder()
                .expireAfterWrite(conf.getBookieQuarantineTimeSeconds(), TimeUnit.SECONDS)
                .removalListener(new RemovalListener<BookieSocketAddress, Boolean>() {

                    @Override
                    public void onRemoval(RemovalNotification<BookieSocketAddress, Boolean> bookie) {
                        log.info("Bookie {} is no longer quarantined", bookie.getKey());
                    }

                }).build();
    }

    public Set<BookieSocketAddress> getBookies() throws BKException {
        try {
            return FutureUtils.result(registrationClient.getWritableBookies(), EXCEPTION_FUNC).getValue();
        } catch (BKInterruptedException ie) {
            Thread.currentThread().interrupt();
            throw ie;
        }
    }

    public Set<BookieSocketAddress> getReadOnlyBookies() throws BKException {
        try {
            return FutureUtils.result(registrationClient.getReadOnlyBookies(), EXCEPTION_FUNC).getValue();
        } catch (BKInterruptedException ie) {
            Thread.currentThread().interrupt();
            throw ie;
        }
    }

    // this callback is already not executed in zookeeper thread
    private synchronized void processWritableBookiesChanged(Set<BookieSocketAddress> newBookieAddrs) {
        // Update watcher outside ZK callback thread, to avoid deadlock in case some other
        // component is trying to do a blocking ZK operation
        this.writableBookies = newBookieAddrs;
        placementPolicy.onClusterChanged(newBookieAddrs, readOnlyBookies);
        // we don't need to close clients here, because:
        // a. the dead bookies will be removed from topology, which will not be used in new ensemble.
        // b. the read sequence will be reordered based on znode availability, so most of the reads
        //    will not be sent to them.
        // c. the close here is just to disconnect the channel, which doesn't remove the channel from
        //    from pcbc map. we don't really need to disconnect the channel here, since if a bookie is
        //    really down, PCBC will disconnect itself based on netty callback. if we try to disconnect
        //    here, it actually introduces side-effects on case d.
        // d. closing the client here will affect latency if the bookie is alive but just being flaky
        //    on its znode registration due zookeeper session expire.
        // e. if we want to permanently remove a bookkeeper client, we should watch on the cookies' list.
        // if (bk.getBookieClient() != null) {
        //     bk.getBookieClient().closeClients(deadBookies);
        // }
    }

    private synchronized void processReadOnlyBookiesChanged(Set<BookieSocketAddress> readOnlyBookies) {
        this.readOnlyBookies = readOnlyBookies;
        placementPolicy.onClusterChanged(writableBookies, readOnlyBookies);
    }
=======
public interface BookieWatcher {
    Set<BookieId> getBookies() throws BKException;
    Set<BookieId> getAllBookies() throws BKException;
    Set<BookieId> getReadOnlyBookies() throws BKException;
    BookieAddressResolver getBookieAddressResolver();
>>>>>>> 2346686c3b8621a585ad678926adf60206227367

    /**
     * Determine if a bookie should be considered unavailable.
     *
     * @param id
     *          Bookie to check
     * @return whether or not the given bookie is unavailable
     */
<<<<<<< HEAD
    public void initialBlockingBookieRead() throws BKException {
        CompletableFuture<?> writable;
        CompletableFuture<?> readonly;
        synchronized (this) {
            if (initialReadonlyBookiesFuture == null) {
                assert initialWritableBookiesFuture == null;

                writable = this.registrationClient.watchWritableBookies(
                            bookies -> processWritableBookiesChanged(bookies.getValue()));

                readonly = this.registrationClient.watchReadOnlyBookies(
                            bookies -> processReadOnlyBookiesChanged(bookies.getValue()));
                initialWritableBookiesFuture = writable;
                initialReadonlyBookiesFuture = readonly;
            } else {
                writable = initialWritableBookiesFuture;
                readonly = initialReadonlyBookiesFuture;
            }
        }

        try {
            FutureUtils.result(writable, EXCEPTION_FUNC);
        } catch (BKInterruptedException ie) {
            Thread.currentThread().interrupt();
            throw ie;
        }
        try {
            FutureUtils.result(readonly, EXCEPTION_FUNC);
        } catch (BKInterruptedException ie) {
            Thread.currentThread().interrupt();
            throw ie;
        } catch (Exception e) {
            log.error("Failed getReadOnlyBookies: ", e);
        }
    }
=======
    boolean isBookieUnavailable(BookieId id);
>>>>>>> 2346686c3b8621a585ad678926adf60206227367

    /**
     * Create an ensemble with given <i>ensembleSize</i> and <i>writeQuorumSize</i>.
     *
     * @param ensembleSize
     *          Ensemble Size
     * @param writeQuorumSize
     *          Write Quorum Size
     * @return list of bookies for new ensemble.
     * @throws BKNotEnoughBookiesException
     */
    List<BookieId> newEnsemble(int ensembleSize, int writeQuorumSize,
                                          int ackQuorumSize, Map<String, byte[]> customMetadata)
            throws BKNotEnoughBookiesException;

    /**
     * Choose a bookie to replace bookie <i>bookieIdx</i> in <i>existingBookies</i>.
     * @param existingBookies
     *          list of existing bookies.
     * @param bookieIdx
     *          index of the bookie in the list to be replaced.
     * @return the bookie to replace.
     * @throws BKNotEnoughBookiesException
     */
    BookieId replaceBookie(int ensembleSize, int writeQuorumSize, int ackQuorumSize,
                                      Map<String, byte[]> customMetadata,
                                      List<BookieId> existingBookies, int bookieIdx,
                                      Set<BookieId> excludeBookies)
            throws BKNotEnoughBookiesException;


    /**
     * Quarantine <i>bookie</i> so it will not be preferred to be chosen for new ensembles.
     * @param bookie
     */
    void quarantineBookie(BookieId bookie);
}
