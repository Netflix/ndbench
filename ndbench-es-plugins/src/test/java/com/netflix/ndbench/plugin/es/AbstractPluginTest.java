package com.netflix.ndbench.plugin.es;

import com.netflix.ndbench.core.config.IConfiguration;
import com.netflix.ndbench.core.discovery.IClusterDiscovery;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;


/**
 * Enables integration tests to run such that Docker container initialization can be short-circuited in favor
 * of running a full Elasticsearch distribution locally, where such distribution is listening on standard ports.
 * <p>
 * To suppress start up of Elasticsearch in Docker, set the environment variable ES_NDBENCH_NO_DOCKER.
 * The main reason you would want to do this is that the current Docker configuration has some issues with
 * being run so that requests can be routed through an HTTP traffic proxy -- which is useful for debugging.
 */
public class AbstractPluginTest {
    private static final String DEFAULT_DOC_TYPE = "default";

    static class MockServiceDiscoverer implements IClusterDiscovery {
        private int port;

        MockServiceDiscoverer(int port) {
            this.port = port;
        }

        @Override
        public List<String> getApps() {
            return Collections.emptyList();
        }

        @Override
        public List<String> getEndpoints(String appName, int defaultPort) {
            return Collections.singletonList("localhost:" + port);
        }
    }

    protected static EsConfig getConfig(final int portNum,
                                        @Nullable final String hostName,
                                        final String indexName,
                                        final boolean isBulkWrite,
                                        final float maxAcceptableWriteFailures,
                                        final int indexRollsPerHour) {
        return new EsConfig() {
            @Override
            public String getCluster() {
                return "elasticsearch";
            }

            @Override
            public String getHostName() {
                return hostName;
            }

            @Override
            public Boolean isHttps() {
                return false;
            }

            @Override
            public String getIndexName() {
                return indexName;
            }

            @Override
            public String getDocumentType() { return DEFAULT_DOC_TYPE; }

            @Override
            public Integer getRestClientPort() {
                return portNum;
            }

            @Override
            public Integer getBulkWriteBatchSize() {
                return 5;
            }

            @Override
            public Boolean isRandomizeStrings() {
                return true;
            }

            @Override
            public Integer getIndexRollsPerDay() {
                return indexRollsPerHour;
            }

            @Override
            public Integer getConnectTimeoutSeconds() {
                return 120;
            }

            @Override
            public Integer getConnectionRequestTimeoutSeconds() {
                return 120;
            }

            @Override
            public Integer getSocketTimeoutSeconds() {
                return 120;
            }

            @Override
            public Integer getMaxRetryTimeoutSeconds() {
                return 120;
            }

        };
    }

    protected static IConfiguration getCoreConfig(final int writeRateLimit,
                                                  final boolean isAutoTuneEnabled,
                                                  final int autoTuneRampPeriodMs,
                                                  final int autoTuneIncrementIntervalMs,
                                                  final int autoTuneFinalRate,
                                                  float maxAcceptableWriteFailures) {
        return new IConfiguration() {
            @Override
            public void initialize() {
            }

            @Override
            public int getNumKeys() {
                return 0;
            }

            @Override
            public int getNumValues() {
                return 0;
            }

            @Override
            public int getDataSize() {
                return 0;
            }

            @Override
            public boolean isPreloadKeys() {
                return false;
            }

            @Override
            public double getZipfExponent() {
                return 0.5;
            }

            @Override
            public int getBackfillKeySlots() {
                return 1;
            }

            @Override
            public boolean isWriteEnabled() {
                return false;
            }

            @Override
            public boolean isReadEnabled() {
                return false;
            }

            @Override
            public int getStatsUpdateFreqSeconds() {
                return 0;
            }

            @Override
            public int getStatsResetFreqSeconds() {
                return 0;
            }

            @Override
            public boolean isUseVariableDataSize() {
                return false;
            }

            @Override
            public int getDataSizeLowerBound() {
                return 0;
            }

            @Override
            public int getDataSizeUpperBound() {
                return 0;
            }

            @Override
            public boolean isGenerateChecksum() {
                return false;
            }

            @Override
            public boolean isValidateChecksum() {
                return false;
            }

            @Override
            public int getReadRateLimit() {
                return 0;
            }

            @Override
            public int getWriteRateLimit() {
                return writeRateLimit;
            }

            @Override
            public boolean isAutoTuneEnabled() {
                return isAutoTuneEnabled;
            }

            @Override
            public Integer getAutoTuneRampPeriodMillisecs() {
                return autoTuneRampPeriodMs;
            }

            @Override
            public Integer getAutoTuneIncrementIntervalMillisecs() {
                return autoTuneIncrementIntervalMs;
            }

            @Override
            public Integer getAutoTuneFinalWriteRate() {
                return autoTuneFinalRate;
            }

            @Override
            public Float getAutoTuneWriteFailureRatioThreshold() {
                return maxAcceptableWriteFailures;
            }

            @Override
            public String getAllowedOrigins() {
                return "";
            }
        };
    }
}

