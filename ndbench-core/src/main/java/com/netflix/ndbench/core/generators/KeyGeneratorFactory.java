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

import com.netflix.ndbench.core.util.LoadPattern;
import org.slf4j.LoggerFactory;

/**
 * @author vchella
 */
public class KeyGeneratorFactory {
    private static final org.slf4j.Logger Logger = LoggerFactory.getLogger(KeyGeneratorFactory.class);

    public KeyGenerator<String> getKeyGenerator(LoadPattern loadPattern, int numKeys, int windowSize, long durationInSec, boolean preLoadKeys, double zipfExponent) {
        Logger.info("Loading "+loadPattern.toString()+" KeyGenerator");

        if (loadPattern.equals(LoadPattern.SLIDING_WINDOW)) {
            return new SlidingWindowStringKeyGenerator(windowSize, durationInSec, preLoadKeys, numKeys);
        }
        else if (loadPattern.equals(LoadPattern.SLIDING_WINDOW_FLIP)) {
            return new SlidingWindowFlipStringKeyGenerator(windowSize, durationInSec, preLoadKeys, numKeys);
        } else if (loadPattern.equals(LoadPattern.ZIPFIAN)) {
            return new ZipfianStringKeyGenerator(preLoadKeys, numKeys, zipfExponent);
        } else {
            return new RandomStringKeyGenerator(preLoadKeys, numKeys);
        }
    }

}
