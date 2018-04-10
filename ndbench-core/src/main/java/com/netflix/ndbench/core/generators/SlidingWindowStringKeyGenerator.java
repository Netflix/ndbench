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
public class SlidingWindowStringKeyGenerator implements KeyGenerator<String> {

    private static Logger logger = LoggerFactory.getLogger(SlidingWindowStringKeyGenerator.class);

    private final int windowSize;
    private final long testDurationInSeconds;
    private final int numKeys;
    private final boolean preLoadKeys;

    private final Random kRandom = new Random();

    private long startTime;
    private long endTime;

    private final List<String> keys = new ArrayList<>();

    public SlidingWindowStringKeyGenerator(int windowSize, long testDurationInSeconds, boolean preLoadKeys, int numKeys)
    {
        logger.info("Initialized SlidingWindowKeyGenerator with WindowSize: "+windowSize+", Test Duration (Secs): "+testDurationInSeconds+", NumKeys: "+numKeys);
        this.windowSize = windowSize;
        this.testDurationInSeconds = testDurationInSeconds;
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
        endTime = startTime + (testDurationInSeconds*1000);
    }



    @Override
    public String getNextKey() {


        int min = getCurrentRecord();
//        int max = Math.max(min + this.windowSize,numKeys);
        int max = min + this.windowSize;
        int nextKey = randomnum(min, max);
        logger.debug("NumKeys: "+numKeys+" | CurrentKeySet: [" +min +" - " +max+"] | getNextKey(): "+nextKey);
        return "T"+nextKey;
    }

    @Override
    public boolean hasNextKey() {
        long currentTime = System.currentTimeMillis();
        if ( endTime < currentTime ) {
            logger.info("No more keys to process since endtime :"+endTime+" < currentTime: "+currentTime);
            return false;
        }
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
    private int getCurrentRecord()
    {
        //Get the current time
        long currentTime = System.currentTimeMillis();

        //How far along has the test run?
        long currentDuration=currentTime-startTime;
        //How far along the test are we?
        double currentRelativePosition=(currentDuration/1000d)/testDurationInSeconds;
        //determine the position of the test window
        double currentRecordRaw=currentRelativePosition*(numKeys-windowSize);

        Long currentRecord=Math.round(currentRecordRaw);

        return currentRecord.intValue();
    }

}
