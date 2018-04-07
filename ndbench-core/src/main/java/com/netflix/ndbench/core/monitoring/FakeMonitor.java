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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.SlidingTimeWindowReservoir;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.ndbench.api.plugin.NdBenchMonitor;
import com.netflix.ndbench.core.config.IConfiguration;
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
    private final AtomicReference<Histogram> readHistogram = new AtomicReference<>();
    private final AtomicReference<Histogram> writeHistogram = new AtomicReference<>();

    private final AtomicLong readSuccess = new AtomicLong(0L);
    private final AtomicLong readFailure = new AtomicLong(0L);
    private final AtomicLong writeSuccess = new AtomicLong(0L);
    private final AtomicLong writeFailure = new AtomicLong(0L);
    private final AtomicLong cacheHits = new AtomicLong(0L);
    private final AtomicLong cacheMiss = new AtomicLong(0L);
    private final AtomicLong readRPS = new AtomicLong(0L);
    private final AtomicLong writeRPS = new AtomicLong(0L);

    private final IConfiguration config;

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
            timer.scheduleAtFixedRate(this::setReadWriteHistograms, 1, config.getStatsResetFreqSeconds(),
                    TimeUnit.SECONDS);
            timerRef.set(timer);
        }
    }

    private void setReadWriteHistograms() {
        readHistogram.set(createHistogramFromConfig());
        writeHistogram.set(createHistogramFromConfig());
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
        readHistogram.get().update(duration);
    }

    @Override
    public long getReadLatAvg() {
        return longValueOfDouble(readHistogram.get().getSnapshot().getMean());
    }

    @Override
    public long getReadLatP50() {
        return longValueOfDouble(readHistogram.get().getSnapshot().getMedian());
    }

    @Override
    public long getReadLatP95() {
        return longValueOfDouble(readHistogram.get().getSnapshot().get95thPercentile());
    }

    @Override
    public long getReadLatP99() {
        return longValueOfDouble(readHistogram.get().getSnapshot().get99thPercentile());
    }

    @Override
    public long getReadLatP995() {
        return longValueOfDouble(readHistogram.get().getSnapshot().getValue(.995));
    }

    @Override
    public long getReadLatP999() {
        return longValueOfDouble(readHistogram.get().getSnapshot().get999thPercentile());
    }

    @Override
    public long getWriteLatAvg() {
        return longValueOfDouble(writeHistogram.get().getSnapshot().getMean());
    }

    @Override
    public long getWriteLatP50() {
            return longValueOfDouble(writeHistogram.get().getSnapshot().getMedian());
    }

    @Override
    public long getWriteLatP95() {
        return longValueOfDouble(writeHistogram.get().getSnapshot().get95thPercentile());
    }

    @Override
    public long getWriteLatP99() {
        return longValueOfDouble(writeHistogram.get().getSnapshot().get99thPercentile());
    }

    @Override
    public long getWriteLatP995() {
        return longValueOfDouble(writeHistogram.get().getSnapshot().getValue(0.995));
    }

    @Override
    public long getWriteLatP999() {
        return longValueOfDouble(writeHistogram.get().getSnapshot().get999thPercentile());
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
        writeHistogram.get().update(duration);
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
        setReadWriteHistograms();

    }
    private float getCacheHitRatio() {
        long hits = cacheHits.get();
        long miss = cacheMiss.get();

        if (hits + miss == 0) {
            return 0;
        }

        return (float) (hits * 100L) / (float) (hits + miss);
    }

    private long longValueOfDouble(double d) {
        return Double.valueOf(d).longValue();
    }

    private Histogram createHistogramFromConfig() {
        return new Histogram(new SlidingTimeWindowReservoir(config.getStatsResetFreqSeconds(), TimeUnit.SECONDS));
    }
}
