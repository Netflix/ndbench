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
package com.netflix.ndbench.api.plugin.common;


/**
 * @author vchella
 */
public final class NdBenchConstants {
    public static final String WEBAPP_NAME = "ndbench";
    public static final String PROP_NAMESPACE = (WEBAPP_NAME + ".config") + ".";

    public static final String NUM_KEYS="numKeys";
    public static final String NUM_VALUES="numValues";
    public static final String DATA_SIZE = "dataSize";
    public static final String NUM_WRITERS = "numWriters";
    public static final String NUM_READERS = "numReaders";
    public static final String NUM_BACKFILL = "numBackfill";
    public static final String BACKFILL_START_KEY = "backfillStartKey";
    public static final String WRITE_ENABLED = "writeEnabled";
    public static final String READ_ENABLED = "readEnabled";
    public static final String STATSUPDATE_FREQ_SECONDS = "statsUpdate.freq.seconds";
    public static final String STATS_RESET_FREQ_SECONDS = "statsReset.freq.seconds";
    public static final String USE_VARIABLE_DATASIZE = "useVariableDataSize";
    public static final String DATASIZE_LOWERBOUND = "upperBound";
    public static final String DATASIZE_UPPERBOUND = "lowerBound";
    public static final String USE_STATIC_DATA = "useStaticData";


    public static final String READ_RATE_LIMIT="readRateLimit";
    public static final String WRITE_RATE_LIMIT="writeRateLimit";

    // Use constant so as to avoid hard coded string references in calling code
    public static final String WRITE_RATE_LIMIT_FULL_NAME = PROP_NAMESPACE + WRITE_RATE_LIMIT;

    public static final String CONFIG_CLUSTER_DISCOVERY_NAME="clusters.json";

    public static final String DISCOVERY_ENV="DISCOVERY_ENV";
    public static final String DISCOVERY_ENV_CF="CF";
    public static final String DISCOVERY_ENV_AWS="AWS";
    public static final String DISCOVERY_ENV_AWS_ASG="AWS_ASG";
    public static final String DISCOVERY_ENV_AWS_CONFIG_FILE="CONFIG_FILE";
}
