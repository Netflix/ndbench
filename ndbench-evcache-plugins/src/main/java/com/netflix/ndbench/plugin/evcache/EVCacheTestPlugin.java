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

import static com.netflix.ndbench.api.plugin.common.NdBenchConstants.PROP_NAMESPACE;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.archaius.api.PropertyFactory;
import com.netflix.evcache.EVCache;
import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;

/**
 * @author smadappa
 */
@Singleton
@NdBenchClientPlugin("EVCacheTest")
public class EVCacheTestPlugin implements NdBenchClient{
    private static final Logger log = LoggerFactory.getLogger(EVCacheTestPlugin.class);

    private DataGenerator dataGenerator;
    private static final String ResultOK = "Ok";
    private static final String CacheMiss = null;

    private final EVCache evcache;


    @Inject
    public EVCacheTestPlugin(PropertyFactory propertyFactory, EVCache.Builder builder) {
        final String evcacheAppName = propertyFactory.getProperty(PROP_NAMESPACE + "evcache.name").asString("EVCACHE").get();
        final String evcacheCachePrefix = propertyFactory.getProperty(PROP_NAMESPACE + "evcache.prefix").asString("").get();
        final Integer evcacheTTL = propertyFactory.getProperty(PROP_NAMESPACE + "evcache.ttl").asInteger(900).get();

        this.evcache = builder.setAppName(evcacheAppName).setCachePrefix(evcacheCachePrefix).setDefaultTTL(evcacheTTL.intValue()).build();
    }


    /**
     * Initialize the client
     *
     * @throws Exception
     */
    @Override
    public void init(DataGenerator dataGenerator) throws Exception {
        this.dataGenerator = dataGenerator;
        log.info("Initialized EVCacheTestPlugin");
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
        if(res!=null)
        {
            if(res.isEmpty())
            {
                throw new Exception("Data retrieved is not ok ");
            }
        }
        else
        {
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

    /**
     * shutdown the client
     */
    @Override
    public void shutdown() throws Exception {
        log.info("Shutting down EVCacheTestPlugin");

    }

    /**
     * Get connection info
     */
    @Override
    public String getConnectionInfo() throws Exception {
        return String.format("EVCacheTestPlugin - EVCache : "+evcache);
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
