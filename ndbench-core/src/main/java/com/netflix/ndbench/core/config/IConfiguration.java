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

/**
 * @author vchella
 */
public interface IConfiguration {
    void initialize();

    // SAMPLE DATA CONFIG
    int getNumKeys();
    int getNumValues();
    int getDataSize();


    // NUM WORKERS
    int getNumWriters();
    int getNumReaders();
    int getNumBackfill();
    int getBackfillStartKey();

    // TEST CASE CONFIG
    boolean getWriteEnabled();
    boolean getReadEnabled();

    //Workers Config
    int getStatsUpdateFreqSeconds();
    int getStatsResetFreqSeconds();

    //Tunable configs
    int getReadRateLimit();
    int getWriteRateLimit();

    //DataGenerator Configs
    boolean getUseVariableDataSize();
    int getDataSizeLowerBound();
    int getDataSizeUpperBound();
    boolean getUseStaticData();
}
