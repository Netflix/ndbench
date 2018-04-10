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
package com.netflix.ndbench.plugin.es;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchAbstractClient;
import com.netflix.ndbench.api.plugin.NdBenchMonitor;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
import com.netflix.ndbench.core.config.IConfiguration;
import com.netflix.ndbench.core.discovery.IClusterDiscovery;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

@Singleton
@NdBenchClientPlugin("ES_REST")
public class EsRestPlugin implements NdBenchAbstractClient<WriteResult> {
    static final String RESULT_OK = "Ok";
    public static final int MAX_INDEX_ROLLS_PER_HOUR = 60;

    private final IClusterDiscovery discoverer;
    private final IEsConfig esConfig;
    private final IConfiguration coreConfig;

    // package scope for fields below --  so their values can be used in tests
    private RestClient restClient;

    private EsWriter writer;
    private String ES_HOST_PORT;
    private String ES_INDEX_TYPE_RESOURCE_PATH;
    private String CONNECTION_INFO;
    private Boolean randomizeKeys;        // for testing we don't randomize so we can pick up what we wrote


    @Nullable
    private EsAutoTuner autoTuner;

    private static final Logger logger = LoggerFactory.getLogger(EsRestPlugin.class);
    private static final String DEFAULT_DOC_TYPE = "default";


    @Inject
    public EsRestPlugin(IConfiguration coreConfig, IEsConfig esConfig, IClusterDiscovery discoverer) {

        this(coreConfig, esConfig, discoverer, true);
    }

    EsRestPlugin(IConfiguration coreConfig, IEsConfig esConfig, IClusterDiscovery discoverer, Boolean randomizeKeys) {
        this.esConfig = esConfig;
        this.coreConfig = coreConfig;
        this.discoverer = discoverer;
        this.randomizeKeys = randomizeKeys;
    }


    public String getEsRestEndpoint() {
        return ES_HOST_PORT;
    }


    /**
     * Returns hostname that the benchmark will target if {@link IEsConfig#getHostName()}
     * is defined, otherwise returns result of calling {@link IEsConfig#getCluster() }
     */

    public String getClusterOrHostName() {
        if (StringUtils.isNotBlank(esConfig.getHostName()))
            return esConfig.getHostName();
        else
            return esConfig.getCluster();
    }


    /**
     * Initialize key data structures for plugin, using 'synchronized' to ensure other threads are guaranteed
     * visibility of end result of initializing said structures.
     *
     * @throws Exception
     */
    @Override
    public synchronized void init(DataGenerator dataGenerator) throws Exception {
        if (esConfig.getRestClientPort() == 443 && !esConfig.isHttps()) {
            throw new IllegalArgumentException(
                    "You must set the configuration property 'https' to true if you use the https default port");
        }

        Integer indexRollsPerHour = esConfig.getIndexRollsPerDay();
        if (indexRollsPerHour < 0 || indexRollsPerHour > MAX_INDEX_ROLLS_PER_HOUR) {
            throw new IllegalArgumentException(
                    "The configuration property 'indexRollsPerHour' must be > 0 and <= " + MAX_INDEX_ROLLS_PER_HOUR);
        }

        if (indexRollsPerHour > 0 && 60 % indexRollsPerHour != 0) {
            throw new IllegalArgumentException( "The configuration property 'indexRollsPerHour' must evenly divide 60");
        }
        if (esConfig.getBulkWriteBatchSize() < 0) {
            throw new IllegalArgumentException( "bulkWriteBatchSize can't be negative'");
        }

        RestClientBuilder.RequestConfigCallback callback = requestConfigBuilder -> {
            requestConfigBuilder.setConnectTimeout(esConfig.getConnectTimeoutSeconds() * 1000);
            requestConfigBuilder.setConnectionRequestTimeout(esConfig.getConnectionRequestTimeoutSeconds() * 1000);
            requestConfigBuilder.setSocketTimeout(esConfig.getSocketTimeoutSeconds()* 1000) ;
            return requestConfigBuilder;
        };
        List<HttpHost> endpoints = getEndpoints(discoverer, esConfig);
        HttpHost[] hosts = endpoints.toArray(new HttpHost[0]);
        restClient =
                RestClient.builder(hosts).
                        setMaxRetryTimeoutMillis(esConfig.getMaxRetryTimeoutSeconds() * 1000).
                        setRequestConfigCallback( callback ).
                        build();

        String hostname = endpoints.get(0).getHostName();
        ES_HOST_PORT = String.format("%s://%s:%s", getScheme(), hostname, esConfig.getRestClientPort());
        ES_INDEX_TYPE_RESOURCE_PATH = String.format("/%s/%s", esConfig.getIndexName(), DEFAULT_DOC_TYPE);
        CONNECTION_INFO =
                "Cluster: " + this.getClusterOrHostName() + "\n" +
                        "Test Index URL: " + ES_HOST_PORT + ES_INDEX_TYPE_RESOURCE_PATH;

        writer = new EsWriter(
                esConfig.getIndexName(),
                DEFAULT_DOC_TYPE,
                esConfig.getBulkWriteBatchSize() > 0,
                indexRollsPerHour,
                esConfig.getBulkWriteBatchSize(), esConfig.isRandomizeStrings() ?
                dataGenerator :
                new FakeWordDictionaryBasedDataGenerator(dataGenerator, coreConfig.getDataSize()));

        if (coreConfig.isAutoTuneEnabled()) {
            this.autoTuner = new EsAutoTuner(
                    coreConfig.getAutoTuneRampPeriodMillisecs(),
                    coreConfig.getAutoTuneIncrementIntervalMillisecs(),
                    coreConfig.getWriteRateLimit(),
                    coreConfig.getAutoTuneFinalWriteRate(),
                    coreConfig.getAutoTuneWriteFailureRatioThreshold());
        } else {
            // OK if it is null because it will never be used if ! isAutoTuneEnabled
            // In fact if we initialized it when auto tune is not enabled, then we would enforce needless checks
            // on related parameters that would impose more hassle on the user to configure
            this.autoTuner = null;
        }

        this.randomizeKeys = randomizeKeys;
        logger.info("ES_REST plugin initialized: " + CONNECTION_INFO);
    }

    private String getScheme() {
        return esConfig.isHttps() ? "https" : "http";
    }

    /**
     * Writes either one or many documents to Elasticsearch -- multiple documents will be written
     * if {@link IEsConfig#getBulkWriteBatchSize()}()} is greater than 0, and in this case the
     * exact number to be written per call is defined by the return value of that same method:
     * {@link IEsConfig#getBulkWriteBatchSize()}.
     * <p>
     * Note that the passed-in key will be appended with random string values -- if we were to choose the
     * ids of the docs we write to elastic search from the set of keys that is allocated per numKeys we would
     * end up PUT'ing the same document to elastic search multiple times, and the subsequent time the document
     * were written to elasticearch it would be counted as an update -- not a new document -- with the result that
     * the deleted doc count (as given by /_cat/indices) would go up, and the document count  would stay the
     * same, which would likely be confusing to whoever is running a benchmark.
     */
    @Override
    public WriteResult writeSingle(String key) throws Exception {
        logger.debug("writeSingle: {}", key);

        return writer.writeDocument(restClient, key, randomizeKeys);
    }

    @Override
    public String readSingle(String key) throws Exception {
        logger.debug("readSingle: {}", key);

        String url = getUrlToDocGivenId(key);

        Response response = restClient.performRequest("GET", url);
        logger.debug("http GET to {} resulted in response: {}", url, response);

        int responseCode = response.getStatusLine().getStatusCode();
        if (responseCode != 200) {
            throw new RuntimeException("write operation failed [" + key + "]. response: " + response);
        }

        return RESULT_OK;
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
    public List<WriteResult> writeBulk(final List<String> keys) throws Exception {
        throw new UnsupportedOperationException("bulk operation is not supported");
    }

    @Override
    public void shutdown() throws Exception {
        restClient.close();
    }

    @Override
    public String getConnectionInfo() throws Exception {
        return CONNECTION_INFO;
    }

    @Override
    public String runWorkFlow() throws Exception {
        return null;
    }

    // Will never be called by driver if isAutoTuneEnabled=false -- for that reason autoTuner is allowed to be null.
    // See constructor for details.
    //
    // Note: this method will only be called after the ndbench driver tries to perform a writeSingle operation
    //
    @Override
    public Double autoTuneWriteRateLimit(Double currentRateLimit, List<WriteResult> event, NdBenchMonitor runStats) {
        assert autoTuner != null;
        return autoTuner.recommendNewRate(currentRateLimit, event, runStats);
    }

    private String getUrlToDocGivenId(String key) {
        return ES_INDEX_TYPE_RESOURCE_PATH + "/" + key;
    }

    private List<HttpHost> getEndpoints(IClusterDiscovery discoverer, IEsConfig config) {
        String hostname = config.getHostName();
        List<HttpHost> retval;

        if (StringUtils.isNotBlank(hostname)) {
            retval = ImmutableList.of(new HttpHost(hostname, config.getRestClientPort(), getScheme()));
        } else {
            ArrayList<HttpHost> hosts = new ArrayList<>();

            logger.debug("discovering endpoints of cluster: {}", config.getCluster());
            for (String endpoint : discoverer.getEndpoints(config.getCluster(), config.getRestClientPort())) {
                String[] hostPort = endpoint.split(":");
                hosts.add(new HttpHost(hostPort[0], Integer.parseInt(hostPort[1]), getScheme()));
            }
            if (hosts.isEmpty()) {
                throw new IllegalArgumentException(
                        "failed to discover any endpoints of cluster: " +
                                config.getCluster() + ".  Are you sure it is valid? Maybe check Spinnaker?");
            }

            retval = hosts;
        }

        return retval;
    }
}
