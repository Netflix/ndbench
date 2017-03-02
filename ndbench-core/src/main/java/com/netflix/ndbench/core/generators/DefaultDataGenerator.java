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
package com.netflix.ndbench.core.generators;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.core.config.IConfiguration;
import org.apache.commons.lang.RandomStringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @author vchella
 */

@Singleton
public class DefaultDataGenerator implements DataGenerator {
    private static Logger logger = LoggerFactory.getLogger(DefaultDataGenerator.class);
    protected final IConfiguration config;


    private final List<String> values = new ArrayList<String>();

    private final Random vRandom = new Random();
    private final Random vvRandom = new Random(DateTime.now().getMillis()); // variable value random

    private final String StaticValue;



    @Inject
    public DefaultDataGenerator(IConfiguration config) {
        this.config = config;
        if (config.getUseVariableDataSize()) {
            initialize(config.getDataSizeLowerBound(), config.getDataSizeUpperBound());
        } else {
            initialize();
        }
        StaticValue = getRandomString();
    }

    @Override
    public String getRandomValue() {
        int randomValueIndex = vRandom.nextInt(config.getNumValues());
        return values.get(randomValueIndex);
    }

    @Override
    public Integer getRandomInteger() {
        return vRandom.nextInt();
    }
    @Override
    public Integer getRandomIntegerValue() {
        return vRandom.nextInt(config.getNumValues());
    }


    @Override
    public String getRandomString()
    {
        return RandomStringUtils.randomAlphanumeric(config.getDataSize());
    }


    private void initialize(int lowerBound, int upperBound) {
        if (config.getUseVariableDataSize()) {
            for (int i = 0; i < config.getNumKeys(); i++) {
                if(i%1000==0) {
                    logger.info("Still initializing sample data for variable data size values");
                }
                int valueSize = upperBound;
                if (upperBound > lowerBound) {
                    valueSize = vvRandom.nextInt(upperBound - lowerBound) + lowerBound;
                }

                String value = RandomStringUtils.randomAlphanumeric(valueSize);

                values.add(value);
            }
        }
    }

    private void initialize() {
        for (int i = 0; i < config.getNumValues(); i++) {
            if(i%1000==0) {
                logger.info("Still initializing sample data for values");
            }
            values.add(constructRandomValue());
        }

    }

    private String constructRandomValue() {

        if (config.getUseStaticData()) {
            return StaticValue;
        }

        return getRandomString();

    }

}
