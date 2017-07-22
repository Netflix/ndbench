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
import com.netflix.archaius.api.config.SettableConfig;
import com.netflix.ndbench.api.plugin.NdBenchAbstractClient;
import com.netflix.ndbench.api.plugin.NdBenchMonitor;
import com.netflix.ndbench.core.NdBenchDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

/**
 * @author vchella
 */
public class WriteOperation<W> implements NdBenchDriver.NdBenchOperation {
    private static final Logger Logger = LoggerFactory.getLogger(WriteOperation.class);

    private final NdBenchAbstractClient<W> client;

    public WriteOperation(NdBenchAbstractClient<W> pClient) {
        client = pClient;
    }

    @Override
    public boolean process(NdBenchDriver driver,
                           NdBenchMonitor stats,
                           String key,
                           AtomicReference<RateLimiter> rateLimiter,
                           boolean isAutoTuneEnabled) {
        try {
            Long startTime = System.nanoTime();
            W result = client.writeSingle(key);
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
            stats.incWriteFailure();
            Logger.error("Failed to process NdBench write operation", e);
            return false;
        } finally {

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