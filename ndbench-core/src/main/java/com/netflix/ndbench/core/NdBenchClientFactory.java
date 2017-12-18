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

import java.util.Map;
import java.util.Set;
/**
 * @author vchella
 */
@Singleton
public class NdBenchClientFactory {

    private Map<String, NdBenchAbstractClient<?>> clientMap;

    @Inject
    public NdBenchClientFactory(Map<String, NdBenchAbstractClient<?>> driverMap) {
        this.clientMap = driverMap;
    }

    public NdBenchAbstractClient<?> getClient(String clientName) {
        NdBenchAbstractClient<?> client = clientMap.get(clientName);
        if (client == null) {
            throw new RuntimeException("Client not found: " + clientName);
        }
        return client;
    }

    public Set<String> getClientDrivers() {
        return clientMap.keySet();
    }
}
