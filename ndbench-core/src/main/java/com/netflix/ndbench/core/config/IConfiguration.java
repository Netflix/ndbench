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
package com.netflix.ndbench.core.config;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;
import com.netflix.archaius.api.annotations.PropertyName;
import com.netflix.ndbench.api.plugin.common.NdBenchConstants;

/**
 * @author vchella
 */
@Configuration(prefix = NdBenchConstants.PROP_NAMESPACE)
public interface IConfiguration {

    void initialize();

    // SAMPLE DATA CONFIG
    @DefaultValue("1000")
    int getNumKeys();

    @DefaultValue("100")
    int getNumValues();

    @DefaultValue("128")
    int getDataSize();

    @DefaultValue("false")
    boolean isPreloadKeys();

    @DefaultValue("0.5")
    double getZipfExponent();

    // NUM WORKERS
    default int getNumWriters() {
        return Runtime.getRuntime().availableProcessors() * 4;
    }

    default int getNumReaders() {
        return Runtime.getRuntime().availableProcessors() * 4;
    }

    //Backfill Settings
    @DefaultValue("1")
    //This configuration usually represents number of workers on your ndbench cluster working on backfill
    int getBackfillKeySlots();


    // TEST CASE CONFIG
    @DefaultValue("true")
    boolean isWriteEnabled();

    @DefaultValue("true")
    boolean isReadEnabled();

    //Workers Config
    @DefaultValue("5")
    int getStatsUpdateFreqSeconds();

    @DefaultValue("200")
    int getStatsResetFreqSeconds();


    //DataGenerator Configs
    @DefaultValue("false")
    boolean isUseVariableDataSize();

    @DefaultValue("1000")
    int getDataSizeLowerBound();

    @DefaultValue("5000")
    int getDataSizeUpperBound();


    //Tunable configs
    @DefaultValue("100")
    int getReadRateLimit();


    @DefaultValue("100")
    @PropertyName(name= NdBenchConstants.WRITE_RATE_LIMIT)
    int getWriteRateLimit();


    @DefaultValue("false")
    boolean isAutoTuneEnabled();


    @DefaultValue("60")
    Integer getAutoTuneRampPeriodMillisecs();

    @DefaultValue("1")
    Integer getAutoTuneIncrementIntervalMillisecs();

    @DefaultValue("1000")
    Integer getAutoTuneFinalWriteRate();

    /**
     *
     * Threshold write failure ratio beyond which no auto-tune increase will occur. By default if failure rate is
     * grows larger than 1% auto tune triggered rate increases will cease.
     *
     */
    @DefaultValue("0.01")
    Float getAutoTuneWriteFailureRatioThreshold();

}
