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
package com.netflix.ndbench.plugin.evcache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.evcache.EVCache;
import com.netflix.evcache.pool.EVCacheClientPoolManager;
import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
import com.netflix.ndbench.plugin.evcache.configs.EVCacheConfigs;

import java.util.List;

/**
 * @author smadappa
 */
@Singleton
@NdBenchClientPlugin("EVCacheTest")
public class EVCacheTestPlugin implements NdBenchClient {
    private static final Logger logger = LoggerFactory.getLogger(EVCacheTestPlugin.class);

    private DataGenerator dataGenerator;
    private static final String ResultOK = "Ok";
    private static final String CacheMiss = null;

    private EVCache evcache;
    private final EVCacheClientPoolManager manager;
    private final EVCache.Builder builder;
    private final EVCacheConfigs configs;

    @Inject
    public EVCacheTestPlugin(EVCacheConfigs configs, EVCache.Builder builder, EVCacheClientPoolManager manager) {
        this.manager = manager;
        this.builder = builder;
        this.configs = configs;
    }

    /**
     * Initialize the client
     *
     * @throws Exception
     */
    @Override
    public void init(DataGenerator dataGenerator) throws Exception {
        this.dataGenerator = dataGenerator;
        logger.info("Initialized EVCacheTestPlugin");
        this.evcache = builder.setAppName(configs.getName()).setCachePrefix(configs.getPrefix())
                .setDefaultTTL(configs.getTTL()).build();
        logger.info("Initialized EVCacheTestPlugin");
    }

    /**
     * Perform a single read operation
     *
     * @param key
     * @return
     * @throws Exception
     */
    @Override
    public String readSingle(String key) throws Exception {
        String res = evcache.get(key);
        if (res == null) {
            return CacheMiss;
        }

        return ResultOK;
    }

    /**
     * Perform a single write operation
     *
     * @param key
     * @return
     * @throws Exception
     */
    @Override
    public String writeSingle(String key) throws Exception {
        evcache.set(key, this.dataGenerator.getRandomValue());
        return ResultOK;
    }

    @Override
    public List<String> readBulk(List<String> keys) throws Exception {
        return null;
    }

    @Override
    public List<String> writeBulk(List<String> keys) throws Exception {
        return null;
    }

    /**
     * shutdown the client
     */
    @Override
    public void shutdown() throws Exception {
        logger.info("Shutting down EVCacheClientPoolManager");
        this.manager.shutdown();
    }

    /**
     * Get connection info
     */
    @Override
    public String getConnectionInfo() throws Exception {
        return String.format("EVCacheTestPlugin - EVCache : " + evcache);
    }

    /**
     * Run workflow for functional testing
     *
     * @throws Exception
     */
    @Override
    public String runWorkFlow() throws Exception {
        return null;
    }
}
