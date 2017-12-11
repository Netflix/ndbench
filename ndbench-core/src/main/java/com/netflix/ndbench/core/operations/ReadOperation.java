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
    private static final Logger Logger = LoggerFactory.getLogger(ReadOperation.class);

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
            Long startTime = System.nanoTime();
            List<String> value = new ArrayList<>(keys.size());
            if (keys.size() > 1) {
                // bulk
                value.addAll(client.readBulk(keys));
            } else {
                // single
                value.add(client.readSingle(keys.get(0)));
            }
            monitor.recordReadLatency((System.nanoTime() - startTime)/1000);
            if (value != null) {
                monitor.incCacheHit();
            } else {
                Logger.debug("Miss for key: {}", keys);
                monitor.incCacheMiss();
            }
            monitor.incReadSuccess();
            return true;
        } catch (Exception e) {
            monitor.incReadFailure();
            Logger.error("Failed to process NdBench read operation", e);
            return false;
        } finally {

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