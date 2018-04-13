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
package com.netflix.ndbench.core.monitoring;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.ndbench.api.plugin.NdBenchMonitor;
import com.netflix.ndbench.core.config.IConfiguration;
import com.netflix.ndbench.core.util.EstimatedHistogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author vchella
 */
@Singleton
public class FakeMonitor implements NdBenchMonitor {

    private static final Logger logger = LoggerFactory.getLogger(FakeMonitor.class);

    private final AtomicReference<ScheduledExecutorService> timerRef = new AtomicReference<>(null);

    private final IConfiguration config;

    private final AtomicLong readSuccess = new AtomicLong(0L);
    private final AtomicLong readFailure = new AtomicLong(0L);
    private final AtomicLong writeSuccess = new AtomicLong(0L);
    private final AtomicLong writeFailure = new AtomicLong(0L);
    private final AtomicLong cacheHits = new AtomicLong(0L);
    private final AtomicLong cacheMiss = new AtomicLong(0L);

    private EstimatedHistogram readHistogram = new EstimatedHistogram(180);
    private EstimatedHistogram writeHistogram = new EstimatedHistogram(180);

    private final AtomicLong readRPS = new AtomicLong(0L);
    private final AtomicLong writeRPS = new AtomicLong(0L);


    @Inject
    public FakeMonitor(IConfiguration config)
    {
        this.config = config;
        checkAndInitTimer();
    }
    @Override
    public void initialize() {

    }
    private void checkAndInitTimer() {
        /** CODE TO PERIODICALLY RESET Histograms */
        ScheduledExecutorService timer = timerRef.get();
        if (timer == null) {
            timer = Executors.newScheduledThreadPool(1);
            logger.info(String.format("Initializing NdBenchMonitor with timing counter reset frequency %d seconds",
                    config.getStatsResetFreqSeconds()));
            timer.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    readHistogram.getBuckets(true);
                    writeHistogram.getBuckets(true);
                }
            }, 1, config.getStatsResetFreqSeconds(), TimeUnit.SECONDS);
        }
    }
    @Override
    public void incReadSuccess() {
        readSuccess.incrementAndGet();
    }

    @Override
    public long getReadSuccess() {
        return readSuccess.get();
    }

    @Override
    public void incReadFailure() {
        readFailure.incrementAndGet();
    }

    @Override
    public long getReadFailure() {
        return readFailure.get();
    }

    @Override
    public void incWriteSuccess() {
        writeSuccess.incrementAndGet();
    }

    @Override
    public long getWriteSuccess() {
        return writeSuccess.get();
    }

    @Override
    public void incWriteFailure() {
        writeFailure.incrementAndGet();
    }

    @Override
    public long getWriteFailure() {
        return writeFailure.get();
    }

    @Override
    public void incCacheHit() {
        cacheHits.incrementAndGet();
    }

    @Override
    public long getCacheHits() {
        return cacheHits.get();
    }

    @Override
    public void incCacheMiss() {
        cacheMiss.incrementAndGet();
    }

    @Override
    public long getCacheMiss() {
        return cacheMiss.get();
    }

    @Override
    public void recordReadLatency(long duration) {
        readHistogram.add(duration);
    }

    @Override
    public long getReadLatAvg() {
        return readHistogram.mean();
    }

    @Override
    public long getReadLatP50() {
        return readHistogram.percentile(0.5);
    }

    @Override
    public long getReadLatP95() {
        return readHistogram.percentile(0.95);
    }

    @Override
    public long getReadLatP99() {
        return readHistogram.percentile(0.99);
    }

    @Override
    public long getReadLatP995() {
        return readHistogram.percentile(0.995);
    }

    @Override
    public long getReadLatP999() {
        return readHistogram.percentile(0.999);
    }

    @Override
    public long getWriteLatAvg() {
            return writeHistogram.mean();
    }

    @Override
    public long getWriteLatP50() {
            return writeHistogram.percentile(0.5);
    }

    @Override
    public long getWriteLatP95() {
        return writeHistogram.percentile(0.95);
    }

    @Override
    public long getWriteLatP99() {
        return writeHistogram.percentile(0.99);
    }

    @Override
    public long getWriteLatP995() {
        return writeHistogram.percentile(0.995);
    }

    @Override
    public long getWriteLatP999() {
        return writeHistogram.percentile(0.999);
    }

    @Override
    public long getWriteRPS() {
        return writeRPS.get();
    }

    @Override
    public long getReadRPS() {
        return readRPS.get();
    }

    @Override
    public void setWriteRPS(long writeRPS) {
        this.writeRPS.set(writeRPS);
    }

    @Override
    public void setReadRPS(long readRPS) {
        this.readRPS.set(readRPS);
    }

    @Override
    public void recordWriteLatency(long duration) {
        writeHistogram.add(duration);
    }

    @Override
    public int getCacheHitRatioInt() {
        return (int) getCacheHitRatio();
    }

    @Override
    public void resetStats() {
        readSuccess.set(0L);
        readFailure.set(0L);
        writeSuccess.set(0L);
        writeFailure.set(0L);
        cacheHits.set(0L);
        cacheMiss.set(0L);
        readRPS.set(0L);
        writeRPS.set(0L);
        readHistogram = new EstimatedHistogram();
        writeHistogram = new EstimatedHistogram();

    }
    private float getCacheHitRatio() {
        long hits = cacheHits.get();
        long miss = cacheMiss.get();

        if (hits + miss == 0) {
            return 0;
        }

        return (float) ((float) (hits * 100L) / (float) (hits + miss));
    }
}
