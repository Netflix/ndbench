/*
 *  Copyright 2021 Netflix, Inc.
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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchAbstractClient;
import com.netflix.ndbench.api.plugin.NdBenchMonitor;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
import com.netflix.ndbench.core.config.IConfiguration;
import com.netflix.ndbench.core.discovery.IClusterDiscovery;
import org.apache.commons.lang.StringUtils;
import org.apache.http.StatusLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Singleton
@NdBenchClientPlugin("ES_REST")
public class EsRestPlugin implements NdBenchAbstractClient<WriteResult> {
    private static final Logger logger = LoggerFactory.getLogger(EsRestPlugin.class);

    public static final String RESULT_OK = "Ok";
    public static final int MAX_INDEX_ROLLS_PER_HOUR = 60;

    private final IClusterDiscovery discoverer;
    private final EsConfig config;
    private final IConfiguration coreConfig;

    private EsRestClient restClient;
    private EsWriter writer;

    private String esHostPort;
    private String connectionInfo;
    private Boolean randomizeKeys;    // For testing we don't randomize, so we can pick up what we wrote

    @Nullable
    private EsAutoTuner autoTuner;

    @Inject
    public EsRestPlugin(IConfiguration coreConfig, EsConfig config,
                        IClusterDiscovery discoverer, EsRestClient restClient) {
        this(coreConfig, config, discoverer, restClient, true);
    }

    EsRestPlugin(IConfiguration coreConfig, EsConfig config,
                 IClusterDiscovery discoverer, EsRestClient restClient, Boolean randomizeKeys) {
        this.config = config;
        this.coreConfig = coreConfig;
        this.discoverer = discoverer;
        this.restClient = restClient;
        this.randomizeKeys = randomizeKeys;
    }

    public String getEsRestEndpoint() {
        return esHostPort;
    }

    /**
     * Returns hostname that the benchmark will target if {@link EsConfig#getHostName()}
     * is defined, otherwise returns result of calling {@link EsConfig#getCluster() }
     */
    public String getClusterOrHostName() {
        if (StringUtils.isNotBlank(config.getHostName()))
            return config.getHostName();
        else
            return config.getCluster();
    }

    /**
     * Initialize key data structures for plugin, using "synchronized" to ensure other threads are guaranteed
     * visibility of end result of initializing said structures.
     *
     * @throws Exception
     */
    @Override
    public synchronized void init(DataGenerator dataGenerator) throws Exception {
        if (config.getRestClientPort() == 443 && !config.isHttps()) {
            throw new IllegalArgumentException(
                    "You must set the configuration property \"https\" to true if you use the https default port");
        }

        Integer indexRollsPerHour = config.getIndexRollsPerDay();
        if (indexRollsPerHour < 0 || indexRollsPerHour > MAX_INDEX_ROLLS_PER_HOUR) {
            throw new IllegalArgumentException(
                    "The configuration property \"indexRollsPerHour\" must be > 0 and <= " + MAX_INDEX_ROLLS_PER_HOUR);
        }

        if (indexRollsPerHour > 0 && 60 % indexRollsPerHour != 0) {
            throw new IllegalArgumentException("The configuration property \"indexRollsPerHour\" must evenly divide 60");
        }
        if (config.getBulkWriteBatchSize() < 0) {
            throw new IllegalArgumentException("bulkWriteBatchSize can't be negative'");
        }

        List<URI> hosts = getHosts();
        this.restClient.init(hosts, this.config);

        this.esHostPort = hosts.get(0).toString();
        this.connectionInfo = String.format(
                "Cluster: %s\ntest index URL: %s/%s/%s",
                this.getClusterOrHostName(), esHostPort, config.getIndexName(), config.getDocumentType());

        writer = new EsWriter(
                config.getIndexName(),
                config.getDocumentType(),
                config.getBulkWriteBatchSize() > 0,
                indexRollsPerHour,
                config.getBulkWriteBatchSize(),
                config.isRandomizeStrings() ? dataGenerator : new FakeWordDictionaryBasedDataGenerator(dataGenerator, coreConfig.getDataSize()));

        if (coreConfig.isAutoTuneEnabled()) {
            this.autoTuner = new EsAutoTuner(
                    coreConfig.getAutoTuneRampPeriodMillisecs(),
                    coreConfig.getAutoTuneIncrementIntervalMillisecs(),
                    coreConfig.getWriteRateLimit(),
                    coreConfig.getAutoTuneFinalWriteRate(),
                    coreConfig.getAutoTuneWriteFailureRatioThreshold());
        } else {
            // OK if it is null because it will never be used if !isAutoTuneEnabled
            // In fact if we initialized it when autotune is not enabled, then we would
            // enforce needless checks on related parameters that would impose more
            // hassle on the user to configure.
            this.autoTuner = null;
        }

        logger.info("ES_REST plugin initialized: " + connectionInfo);
    }

    private String getScheme() {
        return config.isHttps() ? "https" : "http";
    }

    /**
     * Writes either one or many documents to Elasticsearch -- multiple documents will be written
     * if {@link EsConfig#getBulkWriteBatchSize()}()} is greater than 0, and in this case the
     * exact number to be written per call is defined by the return value of that same method:
     * {@link EsConfig#getBulkWriteBatchSize()}.
     * <p>
     * Note that the passed-in key will be appended with random string values -- if we were to choose the
     * ids of the docs we write to Elasticsearch from the set of keys that is allocated per numKeys we would
     * end up writing the same document to Elasticsearch multiple times, and the subsequent time the document
     * were written to Elasticsearch it would be counted as an update -- not a new document -- with the result that
     * the deleted doc count (as given by /_cat/indices) would go up, and the document count  would stay the
     * same, which would likely be confusing to whoever is running a benchmark.
     */
    @Override
    public WriteResult writeSingle(String key) throws Exception {
        logger.debug("writeSingle: {}", key);

        return writer.writeDocument(this.restClient, key, randomizeKeys);
    }

    @Override
    public String readSingle(String key) throws Exception {
        logger.debug("readSingle key=[{}]", key);

        StatusLine statusLine = this.restClient.readSingleDocument(config.getIndexName(), config.getDocumentType(), key);

        logger.debug("readSingle key=[{}] resulted in: {}", key, statusLine);

        int responseCode = statusLine.getStatusCode();
        if (responseCode != 200) {
            throw new RuntimeException("Read operation failed for key \"" + key + "\" (HTTP code " + responseCode + ")");
        }

        return RESULT_OK;
    }

    /**
     * Perform a bulk read operation
     *
     * @return a list of response codes
     */
    public List<String> readBulk(final List<String> keys) {
        throw new UnsupportedOperationException("bulk operation is not supported");
    }

    /**
     * Perform a bulk write operation
     *
     * @return a list of response codes
     */
    public List<WriteResult> writeBulk(final List<String> keys) {
        throw new UnsupportedOperationException("bulk operation is not supported");
    }

    @Override
    public void shutdown() throws Exception {
        this.restClient.close();
    }

    @Override
    public String getConnectionInfo() throws Exception {
        return connectionInfo;
    }

    @Override
    public String runWorkFlow() throws Exception {
        return null;
    }

    /**
     * Will never be called by driver if isAutoTuneEnabled=false --
     * for that reason autoTuner is allowed to be null.
     * See constructor for details.
     * <p>
     * Note: this method will only be called after the
     * ndbench driver tries to perform a writeSingle operation
     */
    @Override
    public Double autoTuneWriteRateLimit(Double currentRateLimit, List<WriteResult> event, NdBenchMonitor runStats) {
        assert autoTuner != null;
        return autoTuner.recommendNewRate(currentRateLimit, event, runStats);
    }

    private List<URI> getHosts() {
        List<URI> hosts = Collections.emptyList();

        if (StringUtils.isNotBlank(this.config.getHostName())) {
            logger.debug("Hostname was set, will be using [{}]", this.config.getHostName());

            try {
                hosts = Collections.singletonList(
                        new URI(String.format("%s://%s:%d",
                                getScheme(),
                                this.config.getHostName(),
                                this.config.getRestClientPort())));

            } catch (URISyntaxException e) {
                logger.warn("Failed to parse hostname [{}] port [{}]",
                        this.config.getHostName(),
                        this.config.getRestClientPort());
            }
        } else {
            logger.debug("Discovering endpoints for cluster [{}]", this.config.getCluster());

            hosts = this.discoverer.getEndpoints(this.config.getCluster(), this.config.getRestClientPort())
                    .stream().map(endpoint -> {
                        try {
                            return new URI(String.format("%s://%s", getScheme(), endpoint));
                        } catch (URISyntaxException e) {
                            logger.warn("Failed to parse endpoint [{}]", endpoint);
                            return null;
                        }
                    }).filter(Objects::nonNull).collect(Collectors.toList());
        }

        if (hosts.isEmpty()) {
            throw new IllegalArgumentException(
                    "Failed to discover any endpoints or hostnames for " + this.config.getCluster());
        }

        return hosts;
    }
}
