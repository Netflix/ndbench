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
import com.netflix.ndbench.api.plugin.NdBenchMonitor;
import com.netflix.ndbench.core.config.IConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author vchella
 */
public class RPSCount {
    private static final Logger logger = LoggerFactory.getLogger(RPSCount.class);
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
