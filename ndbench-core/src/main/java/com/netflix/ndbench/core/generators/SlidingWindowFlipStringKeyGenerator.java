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
public class SlidingWindowFlipStringKeyGenerator implements KeyGenerator<String> {

    private static Logger logger = LoggerFactory.getLogger(SlidingWindowFlipStringKeyGenerator.class);

    private final int windowSize;
    private final long windowDurationInMs;
    private final int numKeys;
    private final boolean preLoadKeys;

    private final Random kRandom = new Random();

    private long startTime;

    private final List<String> keys = new ArrayList<String>();

    public SlidingWindowFlipStringKeyGenerator(int windowSize, long windowDurationInMs, boolean preLoadKeys, int numKeys)
    {
        this.windowSize = windowSize;
        this.windowDurationInMs = windowDurationInMs;
        this.numKeys = numKeys;
        this.preLoadKeys = preLoadKeys;
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
        startTime = System.currentTimeMillis();
    }

    @Override
    public String getNextKey() {

        //Algo:
        // 1) Calculate my CurrentKeySet[min-max]
        // 1.1) CurrentKeySet calculation: min=currentWindow*windowSize, max=min+windowSize
        // 2) Get the Random number in my CurrentKeySet
        int currentWindow = getCurrentWindowIndex();
        int min = currentWindow * this.windowSize;
        int max = min + this.windowSize;

        int nextKey = randomnum(min, max);
        logger.debug("Current Window: "+currentWindow+"" + "| CurrentKeySet: [" +min +" - " +max+"] | getNextKey(): "+nextKey);
        return "T"+nextKey;
    }

    @Override
    public boolean hasNextKey() {
        return true;
    }

    @Override
    public boolean isPreLoadKeys() {
        return this.preLoadKeys;
    }

    @Override
    public int getNumKeys() {
        return this.numKeys;
    }

    private int randomnum(int minNum, int maxNum) {
        return kRandom.nextInt(maxNum - minNum) + minNum;
    }

    /*
        Gets the currentWindow, Window number starts from 0.
     */
    private int getCurrentWindowIndex()
    {
        long currentTime = System.currentTimeMillis();
        long currentWindow =((currentTime - startTime) / windowDurationInMs);
        return (int) currentWindow%(numKeys/windowSize);
    }
}
