package com.netflix.ndbench.plugin.dyno;

import com.google.inject.Singleton;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.jedis.DynoJedisClient;
import com.netflix.dyno.jedis.DynoJedisPipeline;
import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;

import org.slf4j.LoggerFactory;
import redis.clients.jedis.Response;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

@Singleton
@NdBenchClientPlugin("DynoGetSetPipeline")
public class DynoJedisGetSetPipeline implements NdBenchClient {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(DynoJedisGetSetPipeline.class);

    private static final int MIN_PIPE_KEYS = 3;
    private static final int MAX_PIPE_KEYS = 10;

    private static final String ClusterName = "dynomite_redis";

    private final AtomicReference<DynoJedisClient> jedisClient = new AtomicReference<DynoJedisClient>(null);

    private DataGenerator dataGenerator;
    private Random randomGenerator = new Random();


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
                hosts.add(new Host("localhost", 8102, Host.Status.Up).setRack("local-dc"));

                return hosts;
            }

        };

        DynoJedisClient jClient = new DynoJedisClient.Builder().withApplicationName(ClusterName)
                .withDynomiteClusterName(ClusterName).withHostSupplier(hSupplier).build();

        jedisClient.set(jClient);

    }

    @Override
    public String readSingle(String key) throws Exception {

        int pipe_keys = randomGenerator.nextInt(MAX_PIPE_KEYS);
        pipe_keys = Math.max(MIN_PIPE_KEYS, pipe_keys);

        DynoJedisPipeline pipeline = jedisClient.get().pipelined();

        Map<String, Response<String>> responses = new HashMap<String, Response<String>>();
        // initialize those many keys
        for (int n = 0; n < pipe_keys; ++n) {
            String nth_key = key + "_" + n;
            // NOTE: Dyno Jedis works on only one key, so we always use the same
            // key in every get operation
            Response<String> resp = pipeline.get(key);
            // We however use the nth key as the key in the hashmap to check
            // individual response on every operation.
            responses.put(nth_key, resp);
        }
        pipeline.sync();

        for (int n = 0; n < pipe_keys; ++n) {
            String nth_key = key + "_" + n;
            Response<String> resp = responses.get(nth_key);
            if (resp == null || resp.get() == null) {
                logger.info("Cache Miss: key:" + key);
                return null;
            } else {
                if (resp.get().startsWith("ERR")) {
                    throw new Exception(String.format("DynoJedisPipeline: error %s", resp.get()));
                }

                if (!isValidResponse(key, resp.get())) {
                    throw new Exception(String.format(
                            "DynoJedisPipeline: pipeline read: value %s does not contain key %s", resp.get(), key));
                }

            }
        }
        return "OK";
    }

    private boolean isValidResponse(String key, String value) {
        return value.startsWith(key) && value.endsWith(key);
    }

    @Override
    public String writeSingle(String key) throws Exception {
        // Create a random key between [0,MAX_PIPE_KEYS]
        int pipe_keys = randomGenerator.nextInt(MAX_PIPE_KEYS);

        // Make sure that the number of keys in the pipeline are at least
        // MIN_PIPE_KEYS
        pipe_keys = Math.max(MIN_PIPE_KEYS, pipe_keys);

        DynoJedisPipeline pipeline = jedisClient.get().pipelined();
        Map<String, Response<String>> responses = new HashMap<String, Response<String>>();

        /**
         * writeSingle returns a single string, so we want to create a
         * StringBuilder to append all the keys in the form "key_n". This is
         * just used to return a single string
         */
        StringBuilder sb = new StringBuilder();
        // Iterate across the number of keys in the pipeline and set
        for (int n = 0; n < pipe_keys; ++n) {
            String nth_key = key + "_" + n;
            sb.append(nth_key);
            Response<String> resp = pipeline.set(key, key + this.dataGenerator.getRandomValue() + key);
            responses.put(nth_key, resp);
        }
        pipeline.sync();

        return sb.toString();
    }

    /**
     * Shutdown the client
     */
    @Override
    public void shutdown() throws Exception {
        if (jedisClient.get() != null) {
            jedisClient.get().stopClient();
            jedisClient.set(null);
        }
    }

    /**
     * Get connection information
     */
    @Override
    public String getConnectionInfo() throws Exception {
        return String.format("DynoJedis Plugin - ConnectionInfo ::Cluster Name - %s", ClusterName);
    }

    @Override
    public String runWorkFlow() throws Exception {
        return null;
    }

}
