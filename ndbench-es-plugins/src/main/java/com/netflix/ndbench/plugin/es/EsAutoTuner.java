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
package com.netflix.ndbench.plugin.es;

import com.netflix.ndbench.api.plugin.NdBenchMonitor;
import com.netflix.ndbench.core.util.ConstantStepWiseRateIncreaser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;


class EsAutoTuner {
    private final ConstantStepWiseRateIncreaser rateIncreaser;
    private final Float autoTuneFailureRatioThreshold;


    private static final Logger logger = LoggerFactory.getLogger(EsAutoTuner.class);


     // captures  time of first auto-tune recommendation request (made via a call to recommendNewRate);
    private volatile long timeOfFirstAutoTuneRequest = -1;


    private volatile boolean crossedAllowedFailureThreshold = false;

    EsAutoTuner(Integer rampPeriodMillisecs,
                Integer incrementIntervalMillisecs,
                Integer initRate,
                Integer finalRate,
                Float autoTuneFailureRatioThreshold) {

        if (autoTuneFailureRatioThreshold <= 0 || autoTuneFailureRatioThreshold >= 1.0) {
            throw new IllegalArgumentException(
                    "autoTuneFailureRatioThreshold must be > 0 and < 1.0. Actual was " + autoTuneFailureRatioThreshold);
        }

        this.autoTuneFailureRatioThreshold = autoTuneFailureRatioThreshold;
        this.rateIncreaser = new ConstantStepWiseRateIncreaser(
                rampPeriodMillisecs,
                incrementIntervalMillisecs,
                initRate,
                finalRate);
    }

    /** Recommends the new write rate potentially taking into account the current rate, the result of the last write and
     *  statistics accumulated to date.   Currently only the success-to-failure ratio is considered and
     *  compared against {@link com.netflix.ndbench.core.config.IConfiguration#getAutoTuneWriteFailureRatioThreshold()}
     *
     * Note that we can ignore the  possible race condition that arises if multiple threads call this method at around
     * the same time.     In this case two threads will be attempting to set timeOfFirstAutoTuneRequest.. but the
     * target values they are using to set this variable  be so close it will not affect the desired behavior of the
     * auto-tuning feature.
     *
     * Note 2: this method will only be called after the ndbench driver tries to perform a writeSingle operation
     */
    double recommendNewRate(double currentRateLimit, List<WriteResult> event, NdBenchMonitor runStats) {
        long currentTime = new Date().getTime();

        if (timeOfFirstAutoTuneRequest < 0) {          // race condition here when multiple writers, but can be ignored
            timeOfFirstAutoTuneRequest = currentTime;
        }

        // Keep rate at current rate if  calculated write failure ratio meets or exceeds configured threshold,
        // But don't even do this check if a divide by zero error would result from calculating the write
        // failure ratio via the formula:   writesFailures / writeSuccesses
        //
        if (runStats.getWriteSuccess() > 0) {
            double calculatedFailureRatio = runStats.getWriteFailure() / (1.0 * runStats.getWriteSuccess());
            if (calculatedFailureRatio >= autoTuneFailureRatioThreshold) {
                crossedAllowedFailureThreshold = true;
                logger.info(
                        "Not considering increase of write rate limit. calculatedFailureRatio={}. threshold={}",
                        calculatedFailureRatio, autoTuneFailureRatioThreshold);
                return currentRateLimit;
            } else {
                // by forgetting we crossed threshold and resetting timeOfFirstAutoTuneRequest we allow the
                // write rate to drop back down to the specified initial value and we get another shot at
                // trying to step wise increase to the max rate.
                if (crossedAllowedFailureThreshold) {
                    crossedAllowedFailureThreshold = false;
                    timeOfFirstAutoTuneRequest  = currentTime;
                }
            }
        }

        return rateIncreaser.getRateForGivenClockTime(timeOfFirstAutoTuneRequest, currentTime);
    }
}
