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

import com.netflix.dyno.jedis.DynoJedisClient;
import com.netflix.dyno.jedis.DynoJedisPipeline;
import com.netflix.ndbench.api.plugin.DataGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Response;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class DynoJedisUtils {
    // private final AtomicReference<DynoJedisClient> jedisClient = new
    // AtomicReference<DynoJedisClient>(null);
    private AtomicReference<DynoJedisClient> jedisClient;
    private static final String ResultOK = "Ok";
    private static final String CacheMiss = null;

    private static final Logger logger = LoggerFactory.getLogger(DynoJedisUtils.class);
    private static Random randomGenerator = new Random();

    public DynoJedisUtils(AtomicReference<DynoJedisClient> jedisClient) {
        this.jedisClient = jedisClient;
    }

    /**
     * This is the non pipelined version of the reads
     * 
     * @param key
     * @return the value of the corresponding key
     * @throws Exception
     */
    public String nonPipelineRead(String key) throws Exception {

        String res = jedisClient.get().get(key);

        if (res != null) {
            if (res.isEmpty()) {
                throw new Exception("Data retrieved is not ok ");
            }
        } else {
            return CacheMiss;
        }

        return ResultOK;

    }

    /**
     * This is the pipelined version of the reads
     * 
     * @param key
     * @return "OK" if everything was read
     * @throws Exception
     */
    public String pipelineRead(String key, int max_pipe_keys, int min_pipe_keys) throws Exception {
        int pipe_keys = randomGenerator.nextInt(max_pipe_keys);
        pipe_keys = Math.max(min_pipe_keys, pipe_keys);

        DynoJedisPipeline pipeline = this.jedisClient.get().pipelined();

        Map<String, Response<String>> responses = new HashMap<String, Response<String>>();
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
                logger.info("Cache Miss on pipelined read: key:" + key);
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

    /**
     * This the pipelined HGETALL
     * 
     * @param key
     * @return the contents of the hash
     * @throws Exception
     */
    public String pipelineReadHGETALL(String key, String hm_key_prefix) throws Exception {
        DynoJedisPipeline pipeline = jedisClient.get().pipelined();
        Response<Map<byte[], byte[]>> resp = pipeline.hgetAll((hm_key_prefix + key).getBytes());
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

    /**
     * Exercising ZRANGE to receive all keys between 0 and MAX_SCORE
     * 
     * @param key
     */
    public String nonPipelineZRANGE(String key, int max_score) {
        StringBuilder sb = new StringBuilder();
        // Return all elements
        Set<String> returnEntries = this.jedisClient.get().zrange(key, 0, -1);
        if (returnEntries.isEmpty()) {
            logger.error("The number of entries in the sorted set are less than the number of entries written");
            return null;
        }

        for (Iterator<String> it = returnEntries.iterator(); it.hasNext();) {
            String f = it.next();
            sb.append(f);
        }

        return sb.toString();
    }

    /**
     * a simple write without a pipeline
     * 
     * @param key
     * @return the result of write (i.e. "OK" if it was successful
     */
    public String nonpipelineWrite(String key, DataGenerator dataGenerator) {
        String value = key + "__" + dataGenerator.getRandomValue() + "__" + key;
        String result = this.jedisClient.get().set(key, value);

        if (!"OK".equals(result)) {
            logger.error("SET_ERROR: GOT " + result + " for SET operation");
            throw new RuntimeException(String.format("DynoJedis: value %s for SET operation is NOT VALID", value, key));

        }

        return result;
    }

    /**
     * pipelined version of the write
     * 
     * @param key
     * @return "key_n"
     */
    public String pipelineWrite(String key, DataGenerator dataGenerator, int max_pipe_keys, int min_pipe_keys)
            throws Exception {
        // Create a random key between [0,MAX_PIPE_KEYS]
        int pipe_keys = randomGenerator.nextInt(max_pipe_keys);

        // Make sure that the number of keys in the pipeline are at least
        // MIN_PIPE_KEYS
        pipe_keys = Math.max(min_pipe_keys, pipe_keys);

        DynoJedisPipeline pipeline = this.jedisClient.get().pipelined();
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
            Response<String> resp = pipeline.set(key, key + dataGenerator.getRandomValue() + key);
            responses.put(nth_key, resp);
        }
        pipeline.sync();

        return sb.toString();
    }

    /**
     * writes with an pipelined HMSET
     * 
     * @param key
     * @return the keys of the hash that was stored.
     */
    public String pipelineWriteHMSET(String key, DataGenerator dataGenerator, String hm_key_prefix) {
        Map<String, String> map = new HashMap<String, String>();
        String hmKey = hm_key_prefix + key;
        map.put((hmKey + "__1"), (key + "__" + dataGenerator.getRandomValue() + "__" + key));
        map.put((hmKey + "__2"), (key + "__" + dataGenerator.getRandomValue() + "__" + key));

        DynoJedisPipeline pipeline = jedisClient.get().pipelined();
        pipeline.hmset(hmKey, map);
        pipeline.expire(hmKey, 3600);
        pipeline.sync();

        return "HMSET:" + hmKey;
    }

    /**
     * This adds MAX_SCORE of elements in a sorted set
     * 
     * @param key
     * @return "OK" if all write operations have succeeded
     * @throws Exception
     */
    public String nonPipelineZADD(String key, DataGenerator dataGenerator, String z_key_prefix, int max_score)
            throws Exception {

        String zKey = z_key_prefix + key;
        int success = 0;
        long returnOp = 0;
        for (int i = 0; i < max_score; i++) {
            returnOp = jedisClient.get().zadd(zKey, i, dataGenerator.getRandomValue() + "__" + zKey);
            success += returnOp;
        }
        // all the above operations will seperate entries
        if (success != max_score - 1) {
            return null;
        }
        return "OK";
    }

    private static boolean isValidResponse(String key, String value) {
        return value.startsWith(key) && value.endsWith(key);
    }
}
