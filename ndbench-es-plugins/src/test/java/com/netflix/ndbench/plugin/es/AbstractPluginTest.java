package com.netflix.ndbench.plugin.es;

import com.google.common.collect.ImmutableList;
import com.netflix.ndbench.core.config.IConfiguration;
import com.netflix.ndbench.core.discovery.IClusterDiscovery;

import javax.annotation.Nullable;
import java.util.List;


/**
 * Enables integration tests to run such  that Docker container initialization can be short circuited in favor
 * of running a full Elasticsearch distribution locally, where such distribution is listening on standard ports
 * (9200 for REST and 9300 for transport.)
 * <p>
 * To suppress start up of Elasticsearch in Docker, set the environment variable  ES_NDBENCH_NO_DOCKER.
 * The main reason you would want to do this is because the current Docker configuration has some issues with
 * being run so that requests can be routed through an HTTP traffic proxy -- which is useful for debugging.
 */
public class AbstractPluginTest {

    static class MockServiceDiscoverer implements IClusterDiscovery {
        int port;

        MockServiceDiscoverer(int port) {
            this.port = port;
        }

        @Override
        public List<String> getApps() {
            return ImmutableList.of();
        }

        @Override
        public List<String> getEndpoints(String appName, int defaultPort) {
            return ImmutableList.of("localhost:" + port);
        }
    }

    protected static IEsConfig getConfig(final int portNum,
                                         @Nullable final String forcedHostName,
                                         final String indexName,
                                         final boolean isBulkWrite,
                                         final float maxAcceptableWriteFailures,
                                         final int indexRollsPerHour) {
        return new IEsConfig() {
            @Override
            public String getCluster() {
                return "elasticsearch";
            }

            @Override
            public String getHostName() {
                return forcedHostName;
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
                                                  final int autoTuneRampPeriodMillisecs,
                                                  final int autoTuneIncremenetIntervalMillisecs,
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
            public int getNumBackfill() {
                return 0;
            }

            @Override
            public int getBackfillStartKey() {
                return 0;
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
            public boolean isUseStaticData() {
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
                return autoTuneRampPeriodMillisecs;
            }

            @Override
            public Integer getAutoTuneIncrementIntervalMillisecs() {
                return autoTuneIncremenetIntervalMillisecs;
            }

            @Override
            public Integer getAutoTuneFinalWriteRate() {
                return autoTuneFinalRate;
            }

            @Override
            public Float getAutoTuneWriteFailureRatioThreshold() {
                return maxAcceptableWriteFailures;
            }
            

        };
    }
}

