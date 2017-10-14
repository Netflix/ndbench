package com.netflix.ndbench.plugin.es;

import com.netflix.ndbench.api.plugin.NdBenchMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;


class EsAutoTuner {
    private final ConstantStepWiseRateIncreaser rateIncreaser;
    private final Float autoTuneFailureRatioThreshold;


    private static final Logger logger = LoggerFactory.getLogger(EsAutoTuner.class);

    /**
     * // captures  time of first auto-tune recommendation request (made via a call to recommendNewRate);
     * TODO - update comment
     * Ignore the  possible race condition that arises if multiple threads call this method at around the same time
     * when an instance is an uninitialized state. Two (or maybe more) instance of ConstantStepWiseRateIncreaser
     * may be concurrently built, but the system clock time to rate mappings will be close enough that it
     * won't really matter -- auto-tuning behavior will work as expected.
     */
    private volatile long timeOfFirstAutoTuneRequest = -1;

    private volatile boolean writeFailureThresholdReached = false;

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

    double recommendNewRate(double currentRateLimit, WriteResult event, NdBenchMonitor runStats) {
        long currentTime = new Date().getTime();

        if (timeOfFirstAutoTuneRequest < 0) {
            timeOfFirstAutoTuneRequest = currentTime;
        }


        if (writeFailureThresholdReached) {
            return currentRateLimit;
        }


        // Keep rate at current rate if  calculated write failure ratio meets or exceeds configured threshold,
        // But don't even do this check if a divide by zero error would result from calculating the write
        // failure ratio via the formula:   writesFailures / writeSuccesses
        //
        if (runStats.getWriteSuccess() > 0) {
            double calculatedFailureRatio = runStats.getWriteFailure() / (1.0 * runStats.getWriteSuccess());
            if (calculatedFailureRatio >= autoTuneFailureRatioThreshold) {
                logger.info(
                        "Not considering increase of write rate limit. calculatedFailureRatio={}. threshold={}",
                        calculatedFailureRatio, autoTuneFailureRatioThreshold);
                writeFailureThresholdReached = true;
                return currentRateLimit;
            }
        }

        return rateIncreaser.getRateForGivenClockTime(timeOfFirstAutoTuneRequest, currentTime);
    }
}
