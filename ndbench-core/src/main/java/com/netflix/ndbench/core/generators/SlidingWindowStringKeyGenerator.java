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

import java.util.Random;

/**
 * @author vchella
 */
public class SlidingWindowStringKeyGenerator implements KeyGenerator<String> {

    private static Logger logger = LoggerFactory.getLogger(SlidingWindowStringKeyGenerator.class);

    private final int windowSize;
    private final long windowDurationInSec;
    private final int numKeys;

    private final Random kRandom = new Random();

    private long startTime;


    public SlidingWindowStringKeyGenerator(int windowSize, long windowDurationInSec, int numKeys)
    {
        this.windowSize = Math.max(windowSize,1);
        this.windowDurationInSec = Math.max(windowDurationInSec,1);
        this.numKeys = numKeys;
    }

    @Override
    public void init() {
        startTime = System.currentTimeMillis();
    }

    @Override
    public String getNextKey() {

        //Algo:
        // 1) Calculate my CurrentKeySet[min-max]
        // 1.1) CurrentKeySet calculation: min=currentWindow*windowSize, max=min+windowSize
        // 2) Get the Random number in my CurrentKeySet

        int min = getCurrentWindowIndex();
        int max = min + this.windowSize;

        int nextKey = randomnum(min, max);
        logger.debug("NumKeys:"+numKeys +" | Current Window: "+min+"" + "| CurrentKeySet: [" +min +" - " +max+"] | getNextKey(): "+nextKey);
        return "T"+nextKey;
    }

    @Override
    public boolean hasNextKey() {
        return true;
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
        long tw =((currentTime - startTime) / (windowDurationInSec*1000));
        return (int) tw %(numKeys-windowSize+1);

    }


}
