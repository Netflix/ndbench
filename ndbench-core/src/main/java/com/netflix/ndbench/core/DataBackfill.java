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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.ndbench.api.plugin.NdBenchAbstractClient;
import com.netflix.ndbench.core.config.IConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author vchella
 */
@Singleton
public class DataBackfill {

    private static final Logger logger = LoggerFactory.getLogger(DataBackfill.class);

    private final IConfiguration config;
    private final AtomicBoolean stop = new AtomicBoolean(false);
    private final AtomicReference<ExecutorService> threadPool = new AtomicReference<ExecutorService>(null);
    private final AtomicInteger missCount = new AtomicInteger(0);
    private final AtomicInteger count = new AtomicInteger(0);

    private final AtomicReference<Future<Void>> futureRef = new AtomicReference<Future<Void>>(null);
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

        final int numThreads = config.getNumBackfill();
        final int backFillStartKey = config.getBackfillStartKey();
        final int numKeysPerThread = (config.getNumKeys() - backFillStartKey) / numThreads;

        logger.info("NUM THREADS: " + numThreads);
        logger.info("NUM KEYS: " + config.getNumKeys());
        logger.info("NUM KEYS PER TH: " + numKeysPerThread);

        initThreadPool(numThreads);

        final CountDownLatch latch = new CountDownLatch(numThreads);

        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            final int startKey = threadId * numKeysPerThread + backFillStartKey;
            final int endKey = startKey + numKeysPerThread;

            threadPool.get().submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    int k = startKey;

                    while (k < endKey && !stop.get()) {
                        try {
                            String key = "T" + k;
                            String result = backfillOperation.process(client, key);
                            logger.info("Backfill Key:" + key + " | Result: " + result);

                            k++;
                            count.incrementAndGet();
                        } catch (Exception e) {
                            logger.error("Retrying after failure", e);
                        }
                    }

                    latch.countDown();
                    logger.info("Stopping datafill writer");
                    return null;
                }
            });
        }


        Future<Void> future = threadPool.get().submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
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
            }
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

        ExecutorService newPool = Executors.newFixedThreadPool(numThreads + 1);
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
    }

    public void shutdown() {
        if (threadPool != null) {
            threadPool.get().shutdownNow();
        }
    }

}
