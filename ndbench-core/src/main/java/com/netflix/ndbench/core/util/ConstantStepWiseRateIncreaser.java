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
package com.netflix.ndbench.core.util;

import javax.annotation.concurrent.Immutable;

/**
 * Ramps a rate from  a specified 'initRate' to a 'finalRate' in constant increments over the course of a
 * given time period as calls are made to {@link #getRateForGivenClockTime}.
 * <p>
 * Internally the class computes a mapping table of clock time values to rates that will be returned by
 * the aforementioned method.  The mapping table uses a base time of zero to start, but each call to
 * {@link #getRateForGivenClockTime} will typically be made using the current epoch time (as Long).
 * Callers of this class should record the epoch time at which they wish to begin the rate increase ramp
 * and specify this recorded  "baseReferenceTime" as the first argument  to {@link #getRateForGivenClockTime}.
 * This method will adjust the target instances clock value table by adding the baseReferenceTime to each entry.
 */
@Immutable
public class ConstantStepWiseRateIncreaser {
    public static final int MAX_STEPS = 10 * 1000;      // don't want to much overhead in searching data structure

    private final int initRate;
    private final int finalRate;
    private final int incrementIntervalMillisecs;
    private final double rateIncrementPerStep;


    /**
     * Returns a step-wise rate increaser which will ramp from 'initRate' to 'finalRate' over the course of
     * 'incrementIntervalMillisecs'.  The number of steps by which the rate increases will be determined by
     * the value rampPeriodMillisecs / incrementIntervalMillisecs (which MUST evaluate to an integral value with
     * no remainder). At each step the rate will increase constantly by (finalRate - initRate)  / number-of-steps.
     */
    public ConstantStepWiseRateIncreaser(int rampPeriodMillisecs,
                                  int incrementIntervalMillisecs,
                                  int initRate,
                                  int finalRate) {
        if (!(initRate >= 0)) {
            throw new IllegalArgumentException("initRate must be >= 0");
        }
        if (!(finalRate > 0)) {
            throw new IllegalArgumentException("finalRate must be > 0");
        }
        if (!(finalRate > initRate)) {
            throw new IllegalArgumentException("finalRate must be > initRate");
        }
        if (!(rampPeriodMillisecs > 0)) {
            throw new IllegalArgumentException("rampPeriodMillisecs must be > 0");
        }
        if (!(incrementIntervalMillisecs > 0)) {
            throw new IllegalArgumentException("incrementIntervalMillisecs must be > 0");
        }
        if (rampPeriodMillisecs % incrementIntervalMillisecs != 0) {
            throw new IllegalArgumentException(
                    "rampPeriodMillisecs should be evenly divisible by incrementIntervalMillisecs");
        }
        if (rampPeriodMillisecs / incrementIntervalMillisecs > MAX_STEPS) {
            throw new IllegalArgumentException(
                    "rampPeriodMillisecs / incrementIntervalMillisecs should not exceed MAX_STEPS (" + MAX_STEPS + ")");
        }

        int numSteps = rampPeriodMillisecs / incrementIntervalMillisecs;
        double spread = (finalRate - initRate) * 1.0;

        this.initRate = initRate;
        this.finalRate = finalRate;
        this.incrementIntervalMillisecs = incrementIntervalMillisecs;
        this.rateIncrementPerStep = spread / numSteps;
    }

    public double getRateForGivenClockTime(long baseReferenceTime, long clockTime) {
        if (baseReferenceTime > clockTime) {
            throw new IllegalArgumentException(
                    "specified baseReferenceTime ("
                            + baseReferenceTime + ") is greater than clockTime (" + clockTime + ")");
        }

        long desiredClockTimeRelativizedToTimeZero = clockTime - baseReferenceTime;
        return Math.min(
                initRate + desiredClockTimeRelativizedToTimeZero / incrementIntervalMillisecs * rateIncrementPerStep,
                this.finalRate);
    }
}
