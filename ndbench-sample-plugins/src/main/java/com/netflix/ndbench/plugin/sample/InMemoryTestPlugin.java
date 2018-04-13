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
package com.netflix.ndbench.plugin.sample;

import com.google.common.collect.Maps;
import com.google.inject.Singleton;
import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;


/**
 * @author vchella
 */
@Singleton
@NdBenchClientPlugin("InMemoryTest")
public class InMemoryTestPlugin implements NdBenchClient{
    private static final Logger logger = LoggerFactory.getLogger(InMemoryTestPlugin.class);

    private final Map<String, String> data = Maps.newConcurrentMap();

    private DataGenerator dataGenerator;
    private static final String ResultOK = "Ok";
    private static final String CacheMiss = null;



    /**
     * Initialize the client
     *
     * @throws Exception
     */
    @Override
    public void init(DataGenerator dataGenerator) throws Exception {
    this.dataGenerator = dataGenerator;
        logger.info("Initialized InMemoryTestPlugin");
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
        String res = data.get(key);
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
        data.put(key, this.dataGenerator.getRandomValue());
        return ResultOK;
    }

    /**
     * Perform a bulk read operation
     * @return a list of response codes
     * @throws Exception
     */
    public List<String> readBulk(final List<String> keys) throws Exception {
        throw new UnsupportedOperationException("bulk operation is not supported");
    }

    /**
     * Perform a bulk write operation
     * @return a list of response codes
     * @throws Exception
     */
    public List<String> writeBulk(final List<String> keys) throws Exception {
        throw new UnsupportedOperationException("bulk operation is not supported");
    }

    /**
     * shutdown the client
     */
    @Override
    public void shutdown() throws Exception {
        logger.info("Shutting down InMemoryTestPlugin");

    }

    /**
     * Get connection info
     */
    @Override
    public String getConnectionInfo() throws Exception {
        return String.format("InMemoryTestPlugin - ConnectionInfo :: InMemoryMap Key Count: "+data.size());
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
