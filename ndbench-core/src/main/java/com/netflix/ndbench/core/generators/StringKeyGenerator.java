/*
 *  Copyright 2018 Netflix, Inc.
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

public abstract class StringKeyGenerator implements KeyGenerator<String> {
    private static Logger logger = LoggerFactory.getLogger(StringKeyGenerator.class);

    protected final List<String> keys;
    protected final int numKeys;
    private final boolean preloadKeys;

    protected StringKeyGenerator(int numKeys, boolean preloadKeys) {
        this.numKeys = numKeys;
        this.preloadKeys = preloadKeys;
        this.keys = preloadKeys ? new ArrayList<>(numKeys) : new ArrayList<>();
    }

    @Override
    public void init() {
        if (this.isPreLoadKeys()) {
            logger.info("Preloading " + numKeys + " keys");
            for (int i = 0; i < getNumKeys(); i++) {
                if (i % 10000 == 0)
                    logger.info("Still initializing sample data for Keys. So far: "+ i+" /"+numKeys);
                keys.add("T" + i);
            }
            logger.info("Preloaded " + numKeys + " keys");
        }
    }

    @Override
    public boolean isPreLoadKeys() {
        return preloadKeys;
    }

    @Override
    public int getNumKeys() {
        return numKeys;
    }

    @Override
    public boolean hasNextKey() {
        return true;
    }
}
