package com.netflix.ndbench.plugin.es;

import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;
import java.util.ArrayList;

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
 * <p>
 * <p>
 * TODO -  mention how auto-tuning rate increases must not start until the driver begins calling writeSingle, and
 * how their might be a delay in the initial construction of a ConstantStepWiseRateIncreaser and
 * the time at which the driver starts making the aforementioned calls.
 * <p>
 * TODO -- update this comment.
 * Ignore the  possible race condition that arises if multiple threads call this method at around the same time
 * when an instance is an uninitialized state. Two (or maybe more) arrays will be concurrently built, but the
 * system clock time to rate mappings will be close enough that it won't really matter -- auto-tuning behavior
 * will work as expected.
 */
@Immutable
public class ConstantStepWiseRateIncreaser {
    public static final int MAX_STEPS = 10 * 1000;      // don't want to much overhead in searching data structure

    private final int indexOfLastInTable;
    private final ImmutableList<Cell> clockTimeToRateTable;

    @Immutable
    private class Cell {
        private final long time;
        private final double rate;

        Cell(long time, double rate) {
            this.time = time;
            this.rate = rate;
        }
    }


    /**
     * Returns a step-wise rate increaser which will ramp from 'initRate' to 'finalRate' over the course of
     * 'incrementIntervalMillisecs'.  The number of steps by which the rate increases will be determined by
     * the value rampPeriodMillisecs / incrementIntervalMillisecs (which MUST evaluate to an integral value with
     * no remainder). At each step the rate will increase constantly by (finalRate - initRate)  / number-of-steps.
     */
    ConstantStepWiseRateIncreaser(int rampPeriodMillisecs,
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


        final ArrayList<Cell> tmpClockTimeToRateTable = new ArrayList<Cell>();

        int numSteps = rampPeriodMillisecs / incrementIntervalMillisecs;
        double spread = (finalRate - initRate) * 1.0;
        double rateIncrementPerStep = spread / numSteps;

        for (int i = 0; i <= numSteps; i++) {
            tmpClockTimeToRateTable.add(
                    new Cell(
                            i * incrementIntervalMillisecs,
                            initRate + (i * rateIncrementPerStep)
                    )
            );
        }

        // initialize high values sentinel cell
        tmpClockTimeToRateTable.add(new Cell(Long.MAX_VALUE, 0 /* dummy never used for rate */));

        indexOfLastInTable = numSteps + 1;
        assert (tmpClockTimeToRateTable.size() == indexOfLastInTable + 1);
        assert (tmpClockTimeToRateTable.get(numSteps).time == rampPeriodMillisecs);
        assert (tmpClockTimeToRateTable.get(numSteps).rate == finalRate);


        ImmutableList.Builder<Cell> cellBuilder = new ImmutableList.Builder<>();
        clockTimeToRateTable = cellBuilder.addAll(tmpClockTimeToRateTable).build();
    }

    /**
     * Build internal array mapping relative-from-zero clock time values to the rate that will be returned once system
     * clock exceeds time value in a particular cell (but is less than the time value in the immediately
     * succeeding cell);
     * <p>
     * Ignore the  possible race condition that arises if multiple threads call this method at around the same time
     * when an instance is an uninitialized state. Two (or maybe more) arrays will be concurrently built, but the
     * system clock time to rate mappings will be close enough that it won't really matter -- auto-tuning behavior
     * will work as expected.
     * <p>
     * TODO -- put this comment somewhere else
     */

    public double getRateForGivenClockTime(long baseReferenceTime, long clockTime) {
        if (baseReferenceTime > clockTime) {
            throw new IllegalArgumentException(
                    "specified baseReferenceTime ("
                            + baseReferenceTime + ") is greater than clockTime (" + clockTime + ")");

        }

        long desiredClockTimeRelativizedToTimeZero = clockTime - baseReferenceTime;
        double retval = -1;

        // Find the first cell 'j' whose immediate successor cell (j+1) has a time that exceeds desiredClockTime,
        // then return rate for cell 'j'
        //
        for (int i = 0; i <= indexOfLastInTable; i++) {
            long timeCeilingFromSuccessor = clockTimeToRateTable.get(i + 1).time;
            if (timeCeilingFromSuccessor > desiredClockTimeRelativizedToTimeZero) {
                retval = clockTimeToRateTable.get(i).rate;
                break;
            }
        }

        assert (retval >= 0);           // this invariant must be maintained, otherwise code is really broken
        return retval;
    }
}
