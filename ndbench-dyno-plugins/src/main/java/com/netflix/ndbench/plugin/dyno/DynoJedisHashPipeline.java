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

import com.google.inject.Singleton;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.jedis.DynoJedisClient;
import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;

import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This pluging performs hash operations (HMSET/HGETALL) inside a pipeline of
 * size MAX_PIPE_KEYS against Dynomite.
 * 
 * @author ipapapa
 *
 */
@Singleton
@NdBenchClientPlugin("DynoHashPipeline")
public class DynoJedisHashPipeline implements NdBenchClient {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(DynoJedisHashPipeline.class);

    private static final String HM_KEY_PREFIX = "HM__";

    private static final String ClusterName = "dynomite_redis";

    private final AtomicReference<DynoJedisClient> jedisClient = new AtomicReference<DynoJedisClient>(null);

    private DataGenerator dataGenerator;

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
                hosts.add(new Host("localhost", 8102, "local-dc", Host.Status.Up));

                return hosts;
            }

        };

        DynoJedisClient jClient = new DynoJedisClient.Builder().withApplicationName(ClusterName)
                .withDynomiteClusterName(ClusterName).withHostSupplier(hSupplier).build();

        jedisClient.set(jClient);
    }

    @Override
    public String readSingle(String key) throws Exception {
        DynoJedisUtils jedisUtils = new DynoJedisUtils(jedisClient);
        return jedisUtils.pipelineReadHGETALL(key, HM_KEY_PREFIX);
    }

    @Override
    public String writeSingle(String key) throws Exception {
        DynoJedisUtils jedisUtils = new DynoJedisUtils(jedisClient);
        return jedisUtils.pipelineWriteHMSET(key, dataGenerator, HM_KEY_PREFIX);
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
    public String runWorkFlow() throws Exception {
        return null;
    }

}
