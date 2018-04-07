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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;


/**
 * @author vchella
 */
public class RandomStringKeyGenerator implements KeyGenerator<String> {
    private static Logger logger = LoggerFactory.getLogger(RandomStringKeyGenerator.class);

    private final Random kRandom = new Random();

    private final List<String> keys = new ArrayList<String>();

    private final int numKeys;
    private final boolean preLoadKeys;

    public RandomStringKeyGenerator(boolean preLoadKeys, int numKeys)
    {
        this.preLoadKeys = preLoadKeys;
        this.numKeys = numKeys;
    }
    @Override
    public void init() {
        if (this.isPreLoadKeys()) {
            for (int i = 0; i < getNumKeys(); i++) {
                if (i % 10000 == 0)
                    logger.info("Still initializing sample data for Keys. So far: "+ i+" /"+numKeys);
                keys.add("T" + i);
            }
        }
    }

    @Override
    public String getNextKey() {
        int randomKeyIndex = kRandom.nextInt(getNumKeys());
        if (isPreLoadKeys()) {
            return keys.get(randomKeyIndex);
        } else {
            return "T" + randomKeyIndex;
        }
    }

    @Override
    public boolean hasNextKey() {
        return true;
    }

    @Override
    public boolean isPreLoadKeys() {
        return preLoadKeys;
    }

    @Override
    public int getNumKeys() {
        return this.numKeys;
    }
}
