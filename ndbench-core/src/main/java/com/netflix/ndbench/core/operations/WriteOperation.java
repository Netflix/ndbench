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

import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.core.NdBenchDriver;
import com.netflix.ndbench.core.monitoring.NdBenchMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author vchella
 */
public class WriteOperation implements NdBenchDriver.NdBenchOperation {
    private static final Logger Logger = LoggerFactory.getLogger(WriteOperation.class);

    private final NdBenchClient client;

    public WriteOperation(NdBenchClient pClient) {
        client = pClient;
    }

    @Override
    public boolean process(NdBenchMonitor stats, String key) {
        try {
            Long startTime = System.nanoTime();
            client.writeSingle(key);
            stats.recordWriteLatency((System.nanoTime() - startTime)/1000);
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