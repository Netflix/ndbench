package com.netflix.ndbench.plugin.dyno;

import com.google.inject.Singleton;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.connectionpool.OperationResult;
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
import java.util.concurrent.atomic.AtomicReference;

@Singleton
@NdBenchClientPlugin("DynoHashPipeline")
public class DynoJedisHashPipeline implements NdBenchClient {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(DynoJedisHashPipeline.class);

    private static final String KeyHash = "KeyHash";
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
        return pipelineReadHGETALL(key);
    }


    private String pipelineReadHGETALL(String key) throws Exception {
        DynoJedisPipeline pipeline = jedisClient.get().pipelined();
        Response<Map<byte[], byte[]>> resp = pipeline.hgetAll((HM_KEY_PREFIX + key).getBytes());
        pipeline.sync();
        if (resp == null || resp.get() == null) {
            logger.info("Cache Miss: key:" + key);
            return null;
        } else {
            StringBuilder sb = new StringBuilder();
            for (byte[] bytes : resp.get().keySet()) {
                if (sb.length() > 0) {
                    sb.append(",");
                }
                sb.append(new String(bytes));
            }
            return "HGETALL:" + sb.toString();
        }
    }

    private boolean isValidResponse(String key, String value) {
        return value.startsWith(key) && value.endsWith(key);
    }

    @Override
    public String writeSingle(String key) throws Exception {
        return pipelineWriteHMSET(key);
    }

    public String pipelineWriteHMSET(String key) {
        Map<byte[], byte[]> map = new HashMap<byte[], byte[]>();
        String hmKey = HM_KEY_PREFIX + key;
        map.put((hmKey + "__1").getBytes(),
                (key + "__" + this.dataGenerator.getRandomValue() + "__" + key).getBytes());
        map.put((hmKey + "__2").getBytes(),
                (key + "__" + this.dataGenerator.getRandomValue() + "__" + key).getBytes());

        DynoJedisPipeline pipeline = jedisClient.get().pipelined();
        pipeline.hmset(hmKey.getBytes(), map);
        pipeline.expire(hmKey, 3600);
        pipeline.sync();

        return "HMSET:" + hmKey;
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
