package com.netflix.ndbench.core.util;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ConstantStepWiseRateIncreaserTest {
    @Test
    public void testGetRateReturnsProperRateRelativeToTimeZero() throws Exception {
        ConstantStepWiseRateIncreaser increaser = new ConstantStepWiseRateIncreaser(100, 10, 0, 10);
        assertThat(increaser.getRateForGivenClockTime(0, 9), is(equalTo(0.0)));
        assertThat(increaser.getRateForGivenClockTime(0, 10), is(equalTo(1.0)));
        assertThat(increaser.getRateForGivenClockTime(0, 99), is(equalTo(9.0)));
        assertThat(increaser.getRateForGivenClockTime(0, 100), is(equalTo(10.0)));
        assertThat(increaser.getRateForGivenClockTime(0, 101), is(equalTo(10.0)));
        assertThat(increaser.getRateForGivenClockTime(0, Long.MAX_VALUE - 1), is(equalTo(10.0)));
    }
}
