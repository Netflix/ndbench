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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.ndbench.core.util.NdBenchConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class NdBenchConfiguration implements IConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(NdBenchConfiguration.class);


    //Defaults
    private static final Integer DEFAULT_NUM_KEYS = 1000;
    private static final Integer DEFAULT_NUM_VALUES = 100;
    private static final Integer DEFAULT_DATA_SIZE = 128;
    private static final Integer DEFAULT_NUM_WRITERS = Runtime.getRuntime().availableProcessors() * 4;
    private static final Integer DEFAULT_NUM_READERS = Runtime.getRuntime().availableProcessors() * 4;
    private static final Integer DEFAULT_NUM_BACKFILL = 1;
    private static final Integer DEFAULT_BACKFILL_START_KEY = 1;
    private static final Boolean DEFAULT_WRITE_ENABLED = true;
    private static final Boolean DEFAULT_READ_ENABLED = true;
    private static final Integer DEFAULT_STATSUPDATE_FREQ_SECONDS = 5;
    private static final Integer DEFAULT_STATS_RESET_FREQ_SECONDS = 200;
    private static final Integer DEFAULT_READ_RATE_LIMIT = 1;
    private static final Integer DEFAULT_WRITE_RATE_LIMIT = 1;
    private static final Boolean DEFAULT_USE_VARIABLE_DATASIZE = false;
    private static final Integer DEFAULT_DATASIZE_LOWERBOUND = 1000;
    private static final Integer DEFAULT_DATASIZE_UPPERBOUND = 5000;
    private static final Boolean DEFAULT_USE_STATIC_DATA = false;

    //Config Prop Names
    private static final String CONFIG_NUM_KEYS = NdBenchConstants.PROP_PREFIX + NdBenchConstants.NUM_KEYS;
    private static final String CONFIG_NUM_VALUES = NdBenchConstants.PROP_PREFIX + NdBenchConstants.NUM_VALUES;
    private static final String CONFIG_DATA_SIZE =  NdBenchConstants.PROP_PREFIX + NdBenchConstants.DATA_SIZE;
    private static final String CONFIG_NUM_WRITERS =  NdBenchConstants.PROP_PREFIX + NdBenchConstants.NUM_WRITERS;
    private static final String CONFIG_NUM_READERS =  NdBenchConstants.PROP_PREFIX + NdBenchConstants.NUM_READERS;
    private static final String CONFIG_NUM_BACKFILL =  NdBenchConstants.PROP_PREFIX + NdBenchConstants.NUM_BACKFILL;
    private static final String CONFIG_BACKFILL_START_KEY =  NdBenchConstants.PROP_PREFIX + NdBenchConstants.BACKFILL_START_KEY;
    private static final String CONFIG_WRITE_ENABLED =  NdBenchConstants.PROP_PREFIX + NdBenchConstants.WRITE_ENABLED;
    private static final String CONFIG_READ_ENABLED =  NdBenchConstants.PROP_PREFIX + NdBenchConstants.READ_ENABLED;
    private static final String CONFIG_STATSUPDATE_FREQ_SECONDS =  NdBenchConstants.PROP_PREFIX + NdBenchConstants.STATSUPDATE_FREQ_SECONDS;
    private static final String CONFIG_STATS_RESET_FREQ_SECONDS =  NdBenchConstants.PROP_PREFIX + NdBenchConstants.STATS_RESET_FREQ_SECONDS;
    private static final String CONFIG_READ_RATE_LIMIT = NdBenchConstants.PROP_PREFIX + NdBenchConstants.READ_RATE_LIMIT;
    private static final String CONFIG_WRITE_RATE_LIMIT = NdBenchConstants.PROP_PREFIX + NdBenchConstants.WRITE_RATE_LIMIT;
    private static final String CONFIG_USE_VARIABLE_DATASIZE = NdBenchConstants.PROP_PREFIX + NdBenchConstants.USE_VARIABLE_DATASIZE;
    private static final String CONFIG_DATASIZE_LOWERBOUND = NdBenchConstants.PROP_PREFIX + NdBenchConstants.DATASIZE_LOWERBOUND;
    private static final String CONFIG_DATASIZE_UPPERBOUND = NdBenchConstants.PROP_PREFIX + NdBenchConstants.DATASIZE_UPPERBOUND;
    private static final String CONFIG_USE_STATIC_DATA = NdBenchConstants.PROP_PREFIX +NdBenchConstants.USE_STATIC_DATA;

    private final IConfigSource config;

    private final DynamicIntProperty NUM_KEYS = DynamicPropertyFactory.getInstance().getIntProperty(CONFIG_NUM_KEYS, DEFAULT_NUM_KEYS);
    private final DynamicIntProperty READ_RATE_LIMIT = DynamicPropertyFactory.getInstance().getIntProperty(CONFIG_READ_RATE_LIMIT, DEFAULT_WRITE_RATE_LIMIT);
    private final DynamicIntProperty WRITE_RATE_LIMIT = DynamicPropertyFactory.getInstance().getIntProperty(CONFIG_WRITE_RATE_LIMIT, DEFAULT_WRITE_RATE_LIMIT);

    @Inject
    public NdBenchConfiguration(IConfigSource config) {
        this.config = config;
    }
    @Override
    public void initialize() {
        this.config.initialize();

    }
    @Override
    public int getNumKeys() {
        return config.get(CONFIG_NUM_KEYS, DEFAULT_NUM_KEYS);
    }

    @Override
    public int getNumValues() {
        return config.get(CONFIG_NUM_VALUES, DEFAULT_NUM_VALUES);
    }

    @Override
    public int getDataSize() {
        return config.get(CONFIG_DATA_SIZE, DEFAULT_DATA_SIZE);
    }

    @Override
    public int getNumWriters() {

        return config.get(CONFIG_NUM_WRITERS, DEFAULT_NUM_WRITERS);
    }

    @Override
    public int getNumReaders() {
        return config.get(CONFIG_NUM_READERS, DEFAULT_NUM_READERS);
    }

    @Override
    public int getNumBackfill() {
        return config.get(CONFIG_NUM_BACKFILL, DEFAULT_NUM_BACKFILL);
    }

    @Override
    public int getBackfillStartKey() {
        return config.get(CONFIG_BACKFILL_START_KEY, DEFAULT_BACKFILL_START_KEY);
    }

    @Override
    public boolean getWriteEnabled() {
        return config.get(CONFIG_WRITE_ENABLED, DEFAULT_WRITE_ENABLED);
    }

    @Override
    public boolean getReadEnabled() {
        return config.get(CONFIG_READ_ENABLED, DEFAULT_READ_ENABLED);
    }

    @Override
    public int getStatsUpdateFreqSeconds() {
        return config.get(CONFIG_STATSUPDATE_FREQ_SECONDS, DEFAULT_STATSUPDATE_FREQ_SECONDS);
    }

    @Override
    public int getStatsResetFreqSeconds() {
        return config.get(CONFIG_STATS_RESET_FREQ_SECONDS, DEFAULT_STATS_RESET_FREQ_SECONDS);
    }
    @Override
    public int getReadRateLimit() {
        return config.get(CONFIG_READ_RATE_LIMIT, DEFAULT_READ_RATE_LIMIT);
    }

    @Override
    public int getWriteRateLimit() {
        return config.get(CONFIG_WRITE_RATE_LIMIT, DEFAULT_WRITE_RATE_LIMIT);
    }



    @Override
    public boolean getUseVariableDataSize() {
        return config.get(CONFIG_USE_VARIABLE_DATASIZE, DEFAULT_USE_VARIABLE_DATASIZE);
    }

    @Override
    public int getDataSizeLowerBound() {
        return config.get(CONFIG_DATASIZE_LOWERBOUND, DEFAULT_DATASIZE_LOWERBOUND);
    }

    @Override
    public int getDataSizeUpperBound() {
        return config.get(CONFIG_DATASIZE_UPPERBOUND, DEFAULT_DATASIZE_UPPERBOUND);
    }


    @Override
    public boolean getUseStaticData() {
        return config.get(CONFIG_USE_STATIC_DATA, DEFAULT_USE_STATIC_DATA);
    }
}