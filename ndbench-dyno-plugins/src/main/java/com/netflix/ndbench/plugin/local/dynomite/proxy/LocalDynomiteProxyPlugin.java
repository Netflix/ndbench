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
package com.netflix.ndbench.plugin.local.dynomite.proxy;

import com.google.inject.Singleton;
import com.netflix.dyno.connectionpool.ConnectionPoolConfiguration.LoadBalancingStrategy;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.connectionpool.OperationResult;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.dyno.jedis.DynoJedisClient;
import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
import com.netflix.ndbench.plugin.dyno.DynoJedis;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author diegopacheco
 */
@Singleton
@NdBenchClientPlugin("LocalDynomiteProxyPlugin")
public class LocalDynomiteProxyPlugin implements NdBenchClient{
    private static final Logger logger = LoggerFactory.getLogger(DynoJedis.class);

    private static final String ResultOK = "Ok";
    private static final String CacheMiss = null;

    private static final String ClusterName = "dynomite_redis";

    private DataGenerator dataGenerator;

    private final AtomicReference<DynoJedisClient> jedisClient = new AtomicReference<DynoJedisClient>(null);


    @Override
    public void init(DataGenerator dataGenerator) throws Exception {
        int dynomitePort = 8102;
        this.dataGenerator = dataGenerator;
        if (jedisClient.get() != null) {
            return;
        }

        logger.info("Initing dyno jedis client");

        logger.info("\nDynomite Cluster: " + ClusterName);


        HostSupplier hSupplier = new HostSupplier() {

            @Override
            public List<Host> getHosts() {

                List<Host> hosts = new ArrayList<Host>();
                hosts.add(new Host("localhost", dynomitePort, "local-dc", Host.Status.Up));

                return hosts;
            }

        };



        DynoJedisClient jClient = new DynoJedisClient.Builder()
                .withApplicationName(ClusterName)
                .withDynomiteClusterName(ClusterName)
                .withHostSupplier(hSupplier)
                .withCPConfig(new ConnectionPoolConfigurationImpl("myCP")
                .withTokenSupplier(new LocalHttpEndpointBasedTokenMapSupplier())
                .setLoadBalancingStrategy(LoadBalancingStrategy.TokenAware))
                .build();

        jedisClient.set(jClient);

    }

    @Override
    public String readSingle(String key) throws Exception {

        String res = jedisClient.get().get(key);

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

    @Override
    public String writeSingle(String key) throws Exception {
        OperationResult<String> result = jedisClient.get().d_set(key, dataGenerator.getRandomValue());

        if (!"OK".equals(result.getResult())) {
            logger.error("SET_ERROR: GOT " + result.getResult() + " for SET operation");
            throw new RuntimeException(String.format("DynoJedis: value %s for SET operation is NOT VALID", key));

        }

        return result.getResult();
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
     *
     */
    @Override
    public void shutdown() throws Exception {
        if (jedisClient.get() != null) {
            jedisClient.get().stopClient();
            jedisClient.set(null);
        }
    }

    /**
     * shutdown the client
     */
    @Override
    public String getConnectionInfo() throws Exception {
        return String.format("DynoJedisPlugin - ConnectionInfo ::Cluster Name - %s", ClusterName);
    }

    @Override
    public String runWorkFlow() throws Exception {
        return null;
    }
}