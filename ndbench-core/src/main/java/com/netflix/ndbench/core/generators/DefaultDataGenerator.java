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

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.core.config.IConfiguration;
import org.joda.time.DateTime;

/**
 * @author vchella
 */

@Singleton
public class DefaultDataGenerator implements DataGenerator
{
    private static Logger logger = LoggerFactory.getLogger(DefaultDataGenerator.class);
    protected final IConfiguration config;
    private final List<String> values = new ArrayList<>();

    private final Random vRandom = new Random();
    private final Random vvRandom = new Random(DateTime.now().getMillis()); // variable value random

    @Inject
    public DefaultDataGenerator(IConfiguration config)
    {
        this.config = config;

        initialize();

        //Schedule a task to upsert/ modify random entries from the pre generated values
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        logger.info("Scheduling a thread to modify random values from generated values data set");
        executor.scheduleAtFixedRate(this::upsertRandomString, 10, 10, TimeUnit.MILLISECONDS);
    }

    @Override
    public String getRandomValue()
    {
        int randomValueIndex = vRandom.nextInt(config.getNumValues());
        return values.get(randomValueIndex);
    }

    @Override
    public Integer getRandomInteger()
    {
        return vRandom.nextInt();
    }

    @Override
    public Integer getRandomIntegerValue()
    {
        return vRandom.nextInt(config.getNumValues());
    }

    @Override
    public String getRandomString()
    {
        return generateRandomString(getValueSize());
    }

    private void initialize()
    {
        Instant start = Instant.now();
        for (int i = 0; i < config.getNumValues(); i++)
        {
            if (i % 1000 == 0)
            {
                logger.info("Still initializing sample data for values. So far: " + i + " /" + config.getNumValues());
            }
            values.add(generateRandomString(getValueSize()));
        }
        Instant end = Instant.now();
        logger.info("Duration to initialize the dataset of random data (ISO-8601 format): " + Duration.between(start, end));
    }

    private int getValueSize()
    {
        if (config.isUseVariableDataSize())
        {
            return vvRandom.nextInt(
            Math.abs(config.getDataSizeUpperBound() - config.getDataSizeLowerBound()))
                   + config.getDataSizeLowerBound();
        }
        return config.getDataSize();
    }

    private void upsertRandomString()
    {
        values.set(vRandom.nextInt(config.getNumValues()), generateRandomString(getValueSize()));
    }

    private String generateRandomString(int length)
    {
        StringBuilder builder = new StringBuilder();
        while (builder.length()<length)
        {
            builder.append(Long.toHexString(vRandom.nextLong()));
        }
        return builder.toString().substring(0,length);
    }
}
