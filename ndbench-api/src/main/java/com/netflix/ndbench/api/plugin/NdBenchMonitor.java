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
package com.netflix.ndbench.api.plugin;

/**
 * Monitoring interface to receive notification of NdBench events. A concrete
 * monitor will make event stats available to a monitoring application and may
 * also log events to a log file.
 *
 * Created with IntelliJ IDEA.
 * Developer: vchella
 * Date: 8/11/16
 */
/**
 * @author vchella
 */
public interface NdBenchMonitor {
    void initialize();
    void incReadSuccess();
    long getReadSuccess();

    void incReadFailure();
    long getReadFailure();

    void incWriteSuccess();
    long getWriteSuccess();

    void incWriteFailure();
    long getWriteFailure();

    void incCacheHit();
    long getCacheHits();

    void incCacheMiss();
    long getCacheMiss();

    void recordReadLatency(long duration);
    long getReadLatAvg();
    long getReadLatP50();
    long getReadLatP95();
    long getReadLatP99();
    long getReadLatP995();
    long getReadLatP999();

    long getWriteLatAvg();
    long getWriteLatP50();
    long getWriteLatP95();
    long getWriteLatP99();
    long getWriteLatP995();
    long getWriteLatP999();

    long getWriteRPS();
    long getReadRPS();
    void setWriteRPS(long writeRPS);
    void setReadRPS(long readRPS);


    void recordWriteLatency(long duration);
    int getCacheHitRatioInt();

     void resetStats();

     default String getDocumentation() {
         return "sublcasses should supply documentation for various fields of this class";
     }
}
