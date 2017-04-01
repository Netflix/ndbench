/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.ndbench.plugin.dyno;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Singleton;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.jedis.DynoJedisClient;
import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchBaseClient;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;

/**
 * This is the extended functional test for Dynomite.
 * 
 * It tests:
 * 
 * 1. GET 2. pipelined GET 3. pipelined HGETALL 4. ZRANGE
 * 
 * 1. SET 2. pipelined SET 3. pipelined HMSET, 4. ZADD
 * 
 * @author ipapapa
 *
 */

@Singleton
@NdBenchClientPlugin("DynoExtFunc")
public class DynoJedisExtFunc extends NdBenchBaseClient {
    private final Logger logger = LoggerFactory.getLogger(DynoJedisExtFunc.class);

    private static final int MIN_PIPE_KEYS = 3;
    private static final int MAX_PIPE_KEYS = 10;
    private static final int MAX_SCORE = 5;
    private static final String HM_KEY_PREFIX = "HM__";
    private static final String Z_KEY_PREFIX = "Z__";

    private static final String ClusterName = "dynomite_redis";

    private DataGenerator dataGenerator;

    private AtomicReference<DynoJedisClient> jedisClient = new AtomicReference<DynoJedisClient>(null);

    @Override
    public String readSingle(String key) throws Exception {
        StringBuilder sb = new StringBuilder();
        String correct = null;
        DynoJedisUtils jedisUtils = new DynoJedisUtils(jedisClient);

        correct = jedisUtils.nonPipelineRead(key);
        if (correct == null)
            return null;

        sb.append("simple get: " + correct + " , ");

        correct = jedisUtils.pipelineRead(key, MAX_PIPE_KEYS, MIN_PIPE_KEYS);
        if (correct == null)
            return null;

        sb.append("pipeline get: " + correct + " , ");

        correct = jedisUtils.pipelineReadHGETALL(key, HM_KEY_PREFIX);
        if (correct == null)
            return null;

        sb.append("pipeline hash: " + correct + " , ");

        correct = jedisUtils.nonPipelineZRANGE(key, MAX_SCORE);
        if (correct == null)
            return null;

        sb.append("sorted set: " + correct + " , ");

        return sb.toString();
    }

    @Override
    public String writeSingle(String key) throws Exception {
        StringBuilder sb = new StringBuilder();
        String correct = null;
        DynoJedisUtils jedisUtils = new DynoJedisUtils(jedisClient);

        correct = jedisUtils.nonpipelineWrite(key, dataGenerator);
        if (correct == null) {
            return null;
        }
        sb.append("simple get: " + correct + " , ");

        correct = jedisUtils.pipelineWrite(key, dataGenerator, MAX_PIPE_KEYS, MIN_PIPE_KEYS);
        if (correct == null)
            return null;

        sb.append("pipeline set: " + correct + " , ");

        correct = jedisUtils.pipelineWriteHMSET(key, dataGenerator, HM_KEY_PREFIX);
        if (correct == null)
            return null;

        sb.append("pipeline HMSET: " + correct + " , ");

        correct = jedisUtils.nonPipelineZADD(key, dataGenerator, Z_KEY_PREFIX, MAX_SCORE);
        if (correct == null)
            return null;

        sb.append("non pipeline ZADD: " + correct + " , ");
        return sb.toString();
    }

    @Override
    public void shutdown() throws Exception {
        if (jedisClient.get() != null) {
            jedisClient.get().stopClient();
            jedisClient.set(null);
        }
    }

    @Override
    public String getConnectionInfo() throws Exception {
        return String.format("Cluster Name - %s", ClusterName);
    }

    @Override
    public void init(DataGenerator dataGenerator) throws Exception {
        this.dataGenerator = dataGenerator;
        if (jedisClient.get() != null) {
            return;
        }

        logger.info("Initing dyno jedis client");

        logger.info("\nDynomite Cluster: " + ClusterName);

        HostSupplier hSupplier = new HostSupplier() {

            @Override
            public Collection<Host> getHosts() {

                List<Host> hosts = new ArrayList<Host>();
                hosts.add(new Host("localhost", 8102, "local-dc",Host.Status.Up));

                return hosts;
            }

        };

        DynoJedisClient jClient = new DynoJedisClient.Builder().withApplicationName(ClusterName)
                .withDynomiteClusterName(ClusterName).withHostSupplier(hSupplier).build();

        jedisClient.set(jClient);

    }
    
    @Override
    public String runWorkFlow() throws Exception {
        return null;
    }

}
