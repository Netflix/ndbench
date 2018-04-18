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

package com.netflix.ndbench.core.operations;

import com.google.common.util.concurrent.RateLimiter;
import com.netflix.ndbench.api.plugin.NdBenchAbstractClient;
import com.netflix.ndbench.api.plugin.NdBenchMonitor;
import com.netflix.ndbench.core.NdBenchDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Operation to write given the bulk size
 *
 * @author vchella, pencal
 */
public class WriteOperation<W> implements NdBenchDriver.NdBenchOperation {
    private static final Logger logger = LoggerFactory.getLogger(WriteOperation.class);

    private final NdBenchAbstractClient<W> client;

    public WriteOperation(NdBenchAbstractClient<W> pClient) {
        this.client = pClient;
    }

    @Override
    public boolean process(NdBenchDriver driver,
                           NdBenchMonitor stats,
                           List<String> keys,
                           AtomicReference<RateLimiter> rateLimiter,
                           boolean isAutoTuneEnabled) {
        try {
            Long startTime = System.nanoTime();
            List<W> result;
            if (keys.size() > 1) {
                // bulk
                result = client.writeBulk(keys);
            } else {
                // single
                result = new ArrayList<>(1);
                result.add(client.writeSingle(keys.get(0)));
            }
            stats.recordWriteLatency((System.nanoTime() - startTime)/1000);

            if (isAutoTuneEnabled) {
                Double newRateLimit;
                double currentRate = rateLimiter.get().getRate();
                if ((newRateLimit = client.autoTuneWriteRateLimit(currentRate, result, stats)) > 0
                        && newRateLimit != currentRate) {
                    driver.updateWriteRateLimit(newRateLimit);
                }
            }
            stats.incWriteSuccess();
            return true;
        } catch (Exception e) {
            if (driver.getIsWriteRunning()) {
                stats.incWriteFailure();
                logger.error("Failed to process NdBench write operation", e);
            } else {
                logger.warn("Caught exception while stopping writes: " + e.getMessage());
            }
            return false;
        }
    }

    @Override
    public boolean isReadType() {
        return false;
    }

    @Override
    public boolean isWriteType() {
        return true;
    }
}