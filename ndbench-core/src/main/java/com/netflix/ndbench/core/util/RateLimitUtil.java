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

package com.netflix.ndbench.core.util;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author vchella
 */
public class RateLimitUtil {

    private static final Logger logger = LoggerFactory.getLogger(RateLimitUtil.class);

    private final AtomicReference<InnerState> ref = new AtomicReference<>(null);
    
    private RateLimitUtil(int rps) {
        this.ref.set(new InnerState(rps));
    }
    
    public static RateLimitUtil create(int n) {
        return new RateLimitUtil(n);
    }
    
    public int getRps() {
        return ref.get().getRps();
    }
    
    public boolean acquire() {
        
        if (ref.get().checkSameSecond()) {
            long timeToSleepMs = ref.get().increment();
            if (timeToSleepMs != -1) {
                try {
                    Thread.sleep(timeToSleepMs);
                    return false;
                } catch (InterruptedException e) {
                    // do nothing here
                    return false;
                }
            } else {
                return true;
            }
            
        } else {
            
            InnerState oldState = ref.get();
            InnerState newState = new InnerState(oldState.limit);
            
            ref.compareAndSet(oldState, newState);
            return false;
        }
    }
    
    
    private class InnerState {
        
        private final AtomicInteger counter = new AtomicInteger();
        private final AtomicLong second = new AtomicLong(0L);
        private final AtomicLong origTime = new AtomicLong(0L);

        private final int limit; 
        
        private InnerState(int limit) {
            this.limit = limit;
            counter.set(0);
            origTime.set(System.currentTimeMillis());
            second.set(origTime.get()/1000);
        }
        
        private boolean checkSameSecond() {
            long time = System.currentTimeMillis();
            return second.get() == time/1000;
        }
        
        private long increment() {
            
            if (counter.get() < limit) {
                counter.incrementAndGet();
                return -1;
            } else {
                return System.currentTimeMillis() - origTime.get();
            }
        }
        
        private int getRps() {
            return limit;
        }
    }
    
     public static class UnitTest { 
            
            @Test
            public void testRate() throws Exception {
            
                int nThreads = 5;
                int expectedRps = 100;

                final RateLimitUtil rateLimiter = RateLimitUtil.create(expectedRps);
                final AtomicBoolean stop = new AtomicBoolean(false);
                final AtomicLong counter = new AtomicLong(0L);
                final CountDownLatch latch = new CountDownLatch(nThreads);
                
                ExecutorService thPool = Executors.newFixedThreadPool(nThreads);
                
                final CyclicBarrier barrier = new CyclicBarrier(nThreads+1);
                
                final AtomicLong end = new AtomicLong(0L);
                
                for (int i=0; i<nThreads; i++) {
                
                    thPool.submit(() -> {
                        barrier.await();
                        while (!stop.get()) {
                            if(rateLimiter.acquire()) {
                                counter.incrementAndGet();
                            }
                        }
                        latch.countDown();
                        return null;
                    });
                }
                
                long start = System.currentTimeMillis();
                barrier.await();
                Thread.sleep(10000);
                stop.set(true);
                latch.await();
                end.set(System.currentTimeMillis());
                thPool.shutdownNow();
                
                long duration = end.get() - start;
                long totalCount = counter.get();
                double resultRps = ((double)(totalCount)/((double)duration/1000.0));
                logger.info("Total Count : " + totalCount + ", duration:  " + duration + ", getSuccess rps: " + resultRps);
                
                double percentageDiff = Math.abs(expectedRps-resultRps)*100/resultRps;
                logger.info("Percentage diff: " + percentageDiff);
                
                Assert.assertTrue(percentageDiff < 12.0);
            }
            
        }
}
