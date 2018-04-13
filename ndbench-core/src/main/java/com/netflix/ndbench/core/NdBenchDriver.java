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

import com.google.common.util.concurrent.RateLimiter;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.archaius.api.config.SettableConfig;
import com.netflix.archaius.api.inject.RuntimeLayer;
import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchAbstractClient;
import com.netflix.ndbench.api.plugin.NdBenchMonitor;
import com.netflix.ndbench.api.plugin.common.NdBenchConstants;
import com.netflix.ndbench.core.config.IConfiguration;
import com.netflix.ndbench.core.generators.KeyGenerator;
import com.netflix.ndbench.core.generators.KeyGeneratorFactory;
import com.netflix.ndbench.core.operations.ReadOperation;
import com.netflix.ndbench.core.operations.WriteOperation;
import com.netflix.ndbench.core.util.LoadPattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author vchella
 */
@Singleton
public class NdBenchDriver {
    private static final Logger logger = LoggerFactory.getLogger(NdBenchDriver.class);
    public static final int TIMEOUT = 5;

    private final AtomicInteger readWorkers = new AtomicInteger(0);
    private final AtomicInteger writeWorkers = new AtomicInteger(0);

    private final AtomicReference<ExecutorService> tpReadRef = new AtomicReference<ExecutorService>(null);
    private final AtomicReference<ExecutorService> tpWriteRef = new AtomicReference<ExecutorService>(null);

    private final AtomicBoolean readsStarted = new AtomicBoolean(false);
    private final AtomicBoolean writesStarted = new AtomicBoolean(false);
    private final AtomicBoolean clientInited = new AtomicBoolean(false);



    private final AtomicReference<RateLimiter> readLimiter;
    private final AtomicReference<RateLimiter> writeLimiter;

    private final AtomicReference<ExecutorService> timerRef = new AtomicReference<ExecutorService>(null);
    private final RPSCount rpsCount;

    private final AtomicReference<NdBenchAbstractClient<?>> clientRef =
            new AtomicReference<NdBenchAbstractClient<?>>(null);

    private final AtomicReference<KeyGenerator> keyGeneratorWriteRef = new AtomicReference<>(null);
    private final AtomicReference<KeyGenerator> keyGeneratorReadRef = new AtomicReference<>(null);


    private final IConfiguration config;
    private final NdBenchMonitor ndBenchMonitor;
    private final DataGenerator dataGenerator;
    private final SettableConfig settableConfig;

    @Inject
    NdBenchDriver(IConfiguration config,
                  NdBenchMonitor ndBenchMonitor,
                  DataGenerator dataGenerator,
                  @RuntimeLayer SettableConfig settableConfig) {

        this.config = config;

        this.ndBenchMonitor = ndBenchMonitor;
        this.readLimiter = new AtomicReference<>();
        this.writeLimiter = new AtomicReference<>();

        this.dataGenerator = dataGenerator;
        this.settableConfig = settableConfig;
        this.rpsCount = new RPSCount(readsStarted, writesStarted, readLimiter, writeLimiter, config, ndBenchMonitor);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                logger.info("*** shutting down NdBench server since JVM is shutting down");
                NdBenchDriver.this.stop();
                try {
                    NdBenchDriver.this.shutdownClient();
                    Thread.sleep(2000);
                } catch (Exception e) {
                    //ignore
                }
            }
        });
    }


    public void start(LoadPattern loadPattern, int windowSize, long windowDurationInSec, int bulkSize) {
        logger.info("Starting Load Test Driver...");
        startWrites(loadPattern, windowSize, windowDurationInSec, bulkSize);
        startReads(loadPattern, windowSize, windowDurationInSec, bulkSize);
    }

    public void startReads(LoadPattern loadPattern, int windowSize, long windowDurationInSec, int bulkSize) {
        if (readsStarted.get()) {
            logger.info("Reads already started ... ignoring");
            return;
        }
        startReadsInternal(loadPattern, windowSize, windowDurationInSec, bulkSize);
    }


    private void startReadsInternal(LoadPattern loadPattern, int windowSize, long windowDurationInSec, int bulkSize) {
        logger.info("Starting NdBenchDriver reads...");
        NdBenchOperation operation;

        operation = new ReadOperation(clientRef.get());

        KeyGeneratorFactory keyGeneratorFactory = new KeyGeneratorFactory();

        KeyGenerator<String> keyGenerator = keyGeneratorFactory.getKeyGenerator(loadPattern,
                config.getNumKeys(), windowSize, windowDurationInSec, config.isPreloadKeys(), config.getZipfExponent());

        keyGeneratorReadRef.set(keyGenerator);

        startOperation(
                config.isReadEnabled(),
                config.getNumReaders(),
                readWorkers,
                tpReadRef,
                readLimiter,
                operation,
                keyGenerator,
                config.isAutoTuneEnabled(),
                bulkSize);
        readsStarted.set(true);
    }

    public void startWrites(LoadPattern loadPattern, int windowSize, long windowDurationInSec, int bulkSize) {
        if (writesStarted.get()) {
            logger.info("Writes already started ... ignoring");
            return;
        }

        startWritesInternal(loadPattern, windowSize, windowDurationInSec, bulkSize);
    }

    private void startWritesInternal(LoadPattern loadPattern, int windowSize, long windowDurationInSec, int bulkSize) {
        logger.info("Starting NdBenchDriver writes...");
        NdBenchOperation operation;

        operation = new WriteOperation(clientRef.get());

        KeyGeneratorFactory keyGeneratorFactory = new KeyGeneratorFactory();

        KeyGenerator<String> keyGenerator = keyGeneratorFactory.getKeyGenerator(loadPattern,
                config.getNumKeys(), windowSize, windowDurationInSec, config.isPreloadKeys(), config.getZipfExponent());

        keyGeneratorWriteRef.set(keyGenerator);

        startOperation(config.isWriteEnabled(),
                config.getNumWriters(),
                writeWorkers,
                tpWriteRef,
                writeLimiter,
                operation,
                keyGenerator,
                config.isAutoTuneEnabled(),
                bulkSize);

        writesStarted.set(true);
    }

    public boolean getIsWriteRunning() {
        ExecutorService tp = tpWriteRef.get();
        if (tp != null) {
            return true;
        }
        return false;
    }

    public boolean getIsReadRunning() {
        ExecutorService tp = tpReadRef.get();
        return tp != null;
    }

    private void startOperation(boolean operationEnabled,
                                int numWorkersConfig,
                                AtomicInteger numWorkers,
                                AtomicReference<ExecutorService> tpRef,
                                final AtomicReference<RateLimiter> rateLimiter,
                                final NdBenchOperation operation,
                                final KeyGenerator<String> keyGenerator,
                                Boolean isAutoTuneEnabled,
                                int bulkSize) {

        if (!operationEnabled) {
            logger.info("Operation : {} not enabled, ignoring", operation.getClass().getSimpleName());
            return;
        }
        keyGenerator.init();
        ExecutorService threadPool = Executors.newFixedThreadPool(numWorkersConfig);

        boolean success = tpRef.compareAndSet(null, threadPool);
        if (!success) {
            throw new RuntimeException("Unknown threadpool when performing tpRef CAS operation");
        }

        logger.info("\n\nWorker threads: " + numWorkersConfig + ", Num Keys: " + config.getNumKeys() + "\n\n");

        for (int i = 0; i < numWorkersConfig; i++) {

            threadPool.submit((Callable<Void>) () -> {

                while (!Thread.currentThread().isInterrupted()) {
                    boolean noMoreKey = false;

                    if ((operation.isReadType() && readsStarted.get()) ||
                            (operation.isWriteType() && writesStarted.get())) {
                        if (rateLimiter.get().tryAcquire()) {

                            final Set<String> keys = new HashSet<>(bulkSize * 2);
                            while (keys.size() < bulkSize) {
                                keys.add(keyGenerator.getNextKey());
                                if (!keyGenerator.hasNextKey()) {
                                    noMoreKey = true;
                                    break;
                                }
                            } // eo keygens

                            operation.process(
                                    NdBenchDriver.this,
                                    ndBenchMonitor,
                                    new ArrayList<>(keys),
                                    rateLimiter,
                                    isAutoTuneEnabled);
                        } // eo rateLimiter tryGet
                    } // eo if read or write

                    if (noMoreKey) {
                        logger.info("No more keys to process, hence stopping this thread.");
                        if (operation.isReadType()) {
                            stopReads();
                        } else if (operation.isWriteType()) {
                            stopWrites();
                        }
                        Thread.currentThread().interrupt();
                        break;
                    } // eo if noMoreKey
                } // eo while thread not interrupted
                logger.info("NdBenchWorker shutting down");
                return null;
            });
            numWorkers.incrementAndGet();
        }
    }

    /**
     * FUNCTIONALITY FOR STOPPING THE WORKERS
     */
    public void stop() {
        stopWrites();
        stopReads();
        if (timerRef != null && timerRef.get() != null) {
            timerRef.get().shutdownNow();
            timerRef.set(null);
        }
        ndBenchMonitor.resetStats();
    }

    public void stopReads() {
        readsStarted.set(false);
        keyGeneratorReadRef.set(null);
        stopOperation(tpReadRef);
    }

    public void stopWrites() {
        writesStarted.set(false);
        keyGeneratorWriteRef.set(null);
        stopOperation(tpWriteRef);
    }

    public void stopOperation(AtomicReference<ExecutorService> tpRef) {

        ExecutorService tp = tpRef.get();
        if (tp == null) {
            logger.warn("Broken reference to threadPool -- unable to stop!");
            return;
        }

        tp.shutdownNow();
        tpRef.set(null);

        logger.info("Attempting to shutdown threadpool");
        while (!tp.isTerminated()) {
            try {
                logger.info("Waiting for worker pool to stop, sleeping for 5 to 10 seconds");

                // Wait a while for existing tasks to terminate
                if (!tp.awaitTermination(TIMEOUT, TimeUnit.SECONDS)) {
                    tp.shutdownNow(); // Cancel currently executing tasks
                    // Wait a while for tasks to respond to being cancelled
                    if (!tp.awaitTermination(TIMEOUT, TimeUnit.SECONDS))
                        logger.error("Error while shutting down executor service : ");
                }
                logger.info("Threadpool has terminated!");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();//preserve the message
                tp.shutdownNow();
                logger.info("Failed to terminate Threadpool! Ignoring.");
                break;
            }
        }
        logger.info("Threadpool has terminated!");
    }

    public interface NdBenchOperation {
        boolean process(NdBenchDriver driver,
                        NdBenchMonitor monitor,
                        List<String> keys,
                        AtomicReference<RateLimiter> rateLimiter,
                        boolean isAutoTuneEnabled);

        boolean isReadType();

        boolean isWriteType();
    }

    public void init(NdBenchAbstractClient<?> client) throws Exception {
        if (!clientInited.get()) {
            try {
                if (clientInited.compareAndSet(false, true)) {
                    client.init(this.dataGenerator); // Exceptions from init method will be caught and clientInited will be reset
                    clientRef.set(client);
                }
            } catch (Exception e) {
                clientInited.compareAndSet(true, false);
                throw new Exception("Exception initializing client", e);
            }

            // Logic for dealing with rate limits
            setWriteRateLimit(config.getWriteRateLimit());
            setReadRateLimit(config.getReadRateLimit());

            checkAndInitTimer();
        }
    }

    public void onWriteRateLimitChange() {
        checkAndInitRateLimit(writeLimiter, config.getWriteRateLimit(), "writeLimiter");
    }

    public void onReadRateLimitChange() {
        checkAndInitRateLimit(readLimiter, config.getReadRateLimit(), "readLimiter");
    }

    public void updateWriteRateLimit(double newLimit) {
        settableConfig.setProperty(NdBenchConstants.WRITE_RATE_LIMIT_FULL_NAME, (int) Math.ceil(newLimit));
        onWriteRateLimitChange();
    }


    private void setWriteRateLimit(int prop) {
        checkAndInitRateLimit(writeLimiter, prop, "writeLimiter");
    }

    private void setReadRateLimit(int prop) {
        checkAndInitRateLimit(readLimiter, prop, "readLimiter");
    }

    private void checkAndInitRateLimit(AtomicReference<RateLimiter> rateLimiter, int property, String prop) {
        RateLimiter oldLimiter = rateLimiter.get();
        if (oldLimiter == null) {
            logger.info("Setting rate Limit for: " + prop + " to: " + property);
            rateLimiter.set(RateLimiter.create(property));
            return;
        }

        int oldLimit = Double.valueOf(oldLimiter.getRate()).intValue();
        int newLimit = property;

        logger.info("oldlimit={} / newLimit={}", oldLimit, newLimit);
        if (oldLimit != newLimit) {
            logger.info("Updating rate Limit for: " + prop + " to: " + newLimit);
            rateLimiter.set(RateLimiter.create(newLimit));
        }
    }

    private void checkAndInitTimer() {
        /** CODE TO PERIODICALLY LOG RPS */
        ExecutorService timer = timerRef.get();
        if (timer == null) {
            timer = Executors.newFixedThreadPool(1);
            timer.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    while (!Thread.currentThread().isInterrupted()) {
                        rpsCount.updateRPS();
                        Thread.sleep(config.getStatsUpdateFreqSeconds() * 1000);
                    }
                    return null;
                }
            });
            timerRef.set(timer);
        }
    }

    public void shutdownClient() throws Exception {
        if (clientInited.get()) {
            clientRef.get().shutdown();
            if (clientInited.compareAndSet(true, false)) {
                clientRef.set(null);
            }
        }
    }

    public String readSingle(String key) throws Exception {
        try {
            return clientRef.get().readSingle(key);
        } catch (Exception e) {
            logger.error("FAILED readSingle ", e);
            throw e;
        }
    }


    public String writeSingle(String key) throws Exception {
        Object result = clientRef.get().writeSingle(key);
        return result == null ? "<null>" : result.toString();
    }

    public NdBenchAbstractClient<?> getClient() {
        return clientRef.get();
    }

    public KeyGenerator getWriteLoadPattern() {
        return keyGeneratorWriteRef.get();
    }

    public KeyGenerator getReadLoadPattern() {
        return keyGeneratorReadRef.get();
    }

    static class RPSCount {
        private final AtomicLong reads = new AtomicLong(0L);
        private final AtomicLong writes = new AtomicLong(0L);
        private final IConfiguration config;
        private final NdBenchMonitor ndBenchMonitor;
        private final AtomicReference<RateLimiter> readLimiter;
        private final AtomicReference<RateLimiter> writeLimiter;
        private final AtomicBoolean readsStarted;
        private final AtomicBoolean writesStarted;

        RPSCount(AtomicBoolean readsStarted,
                 AtomicBoolean writesStarted,
                 AtomicReference<RateLimiter> readLimiter,
                 AtomicReference<RateLimiter> writeLimiter,
                 IConfiguration config,
                 NdBenchMonitor ndBenchMonitor) {

            this.readsStarted = readsStarted;
            this.writesStarted = writesStarted;
            this.readLimiter = readLimiter;
            this.writeLimiter = writeLimiter;
            this.config = config;
            this.ndBenchMonitor = ndBenchMonitor;
        }


        void updateRPS() {
            int secondsFreq = config.getStatsUpdateFreqSeconds();


            long totalReads = ndBenchMonitor.getReadSuccess() + ndBenchMonitor.getReadFailure();
            long totalWrites = ndBenchMonitor.getWriteSuccess() + ndBenchMonitor.getWriteFailure();
            long totalOps = totalReads + totalWrites;
            long totalSuccess = ndBenchMonitor.getReadSuccess() + ndBenchMonitor.getWriteSuccess();

            long readRps = (totalReads - reads.get()) / secondsFreq;
            long writeRps = (totalWrites - writes.get()) / secondsFreq;

            long sRatio = (totalOps > 0) ? (totalSuccess * 100L / (totalOps)) : 0;

            reads.set(totalReads);
            writes.set(totalWrites);
            ndBenchMonitor.setWriteRPS(writeRps);
            ndBenchMonitor.setReadRPS(readRps);

            logger.info("Read avg: "  + (double) ndBenchMonitor.getReadLatAvg() / 1000.0  + "ms, Read RPS: "  + readRps
                    + ", Write avg: " + (double) ndBenchMonitor.getWriteLatAvg() / 1000.0 + "ms, Write RPS: " + writeRps
                    + ", total RPS: " + (readRps + writeRps) + ", Success Ratio: " + sRatio + "%");
            long expectedReadRate = (long) this.readLimiter.get().getRate();
            long expectedwriteRate = (long) this.writeLimiter.get().getRate();
            String bottleneckMsg = "If this occurs consistently the benchmark client could be the bottleneck.";

            if (this.config.isReadEnabled() && readsStarted.get() && readRps < expectedReadRate) {
                logger.warn("Observed Read RPS ({}) less than expected read rate + ({}).\n{}",
                        readRps, expectedReadRate, bottleneckMsg);
            }
            if (this.config.isWriteEnabled() && writesStarted.get() && writeRps < expectedwriteRate) {
                logger.warn("Observed Write RPS ({}) less than expected write rate + ({}).\n{}",
                        writeRps, expectedwriteRate, bottleneckMsg);
            }
        }
    }
}
