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
 * @author vchella
 */
public class ReadOperation implements NdBenchDriver.NdBenchOperation {
    private static final Logger logger = LoggerFactory.getLogger(ReadOperation.class);

    private final NdBenchAbstractClient<?> client;

    public ReadOperation(NdBenchAbstractClient<?> pClient) {
        client = pClient;
    }

    @Override
    public boolean process(NdBenchDriver driver,
                           NdBenchMonitor monitor,
                           List<String> keys,
                           AtomicReference<RateLimiter> ignoredForNow,
                           boolean isAutoTuneEnabled) {
        try {

            if (keys.size() > 1) {
                //Bulk requests
                List<String> values = new ArrayList<>(keys.size());

                Long startTime = System.nanoTime();
                values.addAll(client.readBulk(keys));
                monitor.recordReadLatency((System.nanoTime() - startTime) / 1000);

                for (String value : values) {
                    processCacheStats(value, monitor);
                }
            } else {
                //Single requests

                Long startTime = System.nanoTime();
                String value = client.readSingle(keys.get(0));
                monitor.recordReadLatency((System.nanoTime() - startTime) / 1000);

                processCacheStats(value, monitor);
            }

            monitor.incReadSuccess();
            return true;

        } catch (Exception e) {
            if (driver.getIsReadRunning()) {
                monitor.incReadFailure();
                logger.error("Failed to process NdBench read operation", e);
            } else {
                logger.warn("Caught exception while stopping reads: " + e.getMessage());
            }
            return false;
        }
    }

    private void processCacheStats(String value, NdBenchMonitor monitor)
    {
        if (value != null) {
            monitor.incCacheHit();
        } else {
            monitor.incCacheMiss();
        }
    }

    @Override
    public boolean isReadType() {
        return true;
    }

    @Override
    public boolean isWriteType() {
        return false;
    }
}