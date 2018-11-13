/*
 *  Copyright 2016 Netflix, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.netflix.ndbench.core;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.ndbench.api.plugin.NdBenchAbstractClient;
import com.netflix.ndbench.core.config.IConfiguration;

/**
 * @author vchella
 */
@Singleton
public class DataBackfill {

    private static final Logger logger = LoggerFactory.getLogger(DataBackfill.class);

    private final IConfiguration config;
    private final AtomicBoolean stop = new AtomicBoolean(false);
    private final AtomicReference<ExecutorService> threadPool = new AtomicReference<>(null);
    private final AtomicInteger missCount = new AtomicInteger(0);
    final AtomicInteger count = new AtomicInteger(0);
    private final Random random = new Random();

    private final AtomicReference<Future<Void>> futureRef = new AtomicReference<>(null);
    @Inject
    public DataBackfill(IConfiguration config) {
        this.config = config;
    }

    public void backfill(final NdBenchAbstractClient<?> client) throws Exception {
        backfill(client, new NormalBackfill());
    }

    public void conditionalBackfill(final NdBenchAbstractClient<?> client) throws Exception {
        backfill(client, new ConditionalBackfill());
    }

    public void verifyBackfill(final NdBenchAbstractClient<?> client) throws Exception {
        backfill(client, new VerifyBackfill());
    }

    private void backfill(final NdBenchAbstractClient<?> client, final BackfillOperation backfillOperation) throws Exception {

        long start = System.currentTimeMillis();

        backfillAsync(client, backfillOperation);

        logger.info("Backfiller waiting to finish");
        futureRef.get();
        logger.info("Backfiller latch done! in " + (System.currentTimeMillis() - start) + " ms");
    }

    public void backfillAsync(final NdBenchAbstractClient<?> client) {
        backfillAsync(client, new NormalBackfill());
    }

    private void backfillAsync(final NdBenchAbstractClient<?> client, final BackfillOperation backfillOperation) {
        stop.set(false);

        //Default #Cores*4 so that we can keep the CPUs busy even while waiting on I/O
        final int numThreads = Runtime.getRuntime().availableProcessors() * 4;

        initThreadPool(numThreads);

        List<Pair<Integer, Integer>> keyRanges = getKeyRangesPerThread(numThreads,
                                                                       config.getBackfillKeySlots(),
                                                                       config.getNumKeys());

        final CountDownLatch latch = new CountDownLatch(numThreads);

        for (int i = 0; i < numThreads; i++) {
            final int startKey = keyRanges.get(i).getLeft();
            final int endKey = keyRanges.get(i).getRight();

            threadPool.get().submit(() -> {
                int k = startKey;

                while (k < endKey && !stop.get()) {
                    try {
                        String key = "T" + k;
                        String result = backfillOperation.process(client, key);
                        logger.debug("Backfill Key:" + key + " | Result: " + result);
                        k++;
                        count.incrementAndGet();
                    } catch (Exception e) {
                        logger.error("Retrying after failure", e);
                    }
                }

                latch.countDown();
                logger.info("Stopping datafill writer");
                return null;
            });
        }


        Future<Void> future = threadPool.get().submit(() -> {
            final AtomicBoolean stopCounting = new AtomicBoolean(false);

            while (!Thread.currentThread().isInterrupted() && !stopCounting.get()) {
                logger.info("Backfill so far: " + count.get() + ", miss count: " + missCount.get());
                try {
                    boolean done = latch.await(5000, TimeUnit.MILLISECONDS);
                    if (done) {
                        stopCounting.set(true);
                    }
                } catch (InterruptedException e) {
                    // return from here.
                    stopCounting.set(true);
                }
            }
            logger.info("Stopping datafill status poller");
            return null;
        });

        futureRef.set(future);
    }

    public boolean getIsBackfillRunning() {
        Future<Void> future = futureRef.get();
        if (future != null) {
            if (future.isDone() || future.isCancelled()) {
                return false; // Completed running or cancelled, so currently not running
            }
            return true; //Still running
        }
        return false; //Never started
    }

    private interface BackfillOperation {
        String process(final NdBenchAbstractClient<?> client, final String key) throws Exception;
    }

    private class NormalBackfill implements BackfillOperation {

        @Override
        public String process(NdBenchAbstractClient<?> client, String key) throws Exception {
            Object result =  client.writeSingle(key);
            return result == null ? "<null>"  : result.toString();
        }
    }

    private class ConditionalBackfill implements BackfillOperation {

        @Override
        public String process(NdBenchAbstractClient<?> client, String key) throws Exception {
            String result = client.readSingle(key);
            if (result == null) {
                missCount.incrementAndGet();
                Object writeResult =  client.writeSingle(key);
                return writeResult == null ? "<null>"  : writeResult.toString();
            }
            return "done";
        }
    }

    private class VerifyBackfill implements BackfillOperation {

        @Override
        public String process(NdBenchAbstractClient<?> client, String key) throws Exception {
            Object result =  client.writeSingle(key);
            String value = client.readSingle(key);
            if (value == null) {
                missCount.incrementAndGet();
                return "backfill miss: " + result;
            } else {
                return result == null ? "<null>"  : result.toString();
            }
        }
    }

    private void initThreadPool(int numThreads) {

        if (threadPool.get() != null) {
            throw new RuntimeException("Backfill already started");
        }
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                                      .setNameFormat("ndbench-backfill-pool-%d")
                                      .setDaemon(false).build();
        ExecutorService newPool = Executors.newFixedThreadPool(numThreads + 1, threadFactory);
        boolean success = threadPool.compareAndSet(null, newPool);
        if (!success) {
            newPool.shutdownNow();
            throw new RuntimeException("Backfill already started");
        }
    }

    public void stopBackfill() {
        stop.set(true);
        Future<Void> future = futureRef.get();
        if (future != null) {
            future.cancel(true);
        }
        shutdown();

    }

    public void shutdown() {
        if (threadPool.get() != null) {
            threadPool.get().shutdownNow();
            threadPool.set(null);
        }
    }

    /**
     * This method returns the key range to be back filled in [1 - IConfiguration.getNumKeys()] keyspace.
     * Algorithm to determine keyrange:
     * NumKeys/BackfillKeySlots --> This gives the key range slots to be processed.
     * Each worker randomly picks the slot from above. With low #BackfillKeySlots there is high probability
     * to cover keyspace without any misses.
     * @return
     */
    List<Pair<Integer, Integer>> getKeyRangesPerThread(int numThreads, int keySlots, int numKeys)
    {
        List<Pair<Integer, Integer>> keyRangesPerThread = new LinkedList<>();

        int slotSize = numKeys / keySlots;
        int randomSlot = random.nextInt(keySlots);

        int startKey = randomSlot * slotSize;
        int endKey = startKey + slotSize;

        int numKeysToProcess = endKey - startKey;

        int numKeysPerThread = numKeysToProcess / numThreads;

        logger.info("Num keys (KEYSPACE): {}, Num threads: {}, Num slots: {}", numKeys, numThreads, keySlots);
        logger.info("MyNode: Num keys to be processed: {}, Num keys per thread: {}, My key slot: {}",
                    numKeysToProcess, numKeysPerThread, randomSlot);
        for (int i = 0; i < numThreads; i++)
        {
            int startKeyPerThread = startKey + (i * numKeysPerThread);
            int endKeyPerThread = startKeyPerThread + numKeysPerThread;
            keyRangesPerThread.add(Pair.of(startKeyPerThread, endKeyPerThread));
        }
        return keyRangesPerThread;
    }

}
