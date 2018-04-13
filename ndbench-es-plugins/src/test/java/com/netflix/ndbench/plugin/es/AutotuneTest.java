package com.netflix.ndbench.plugin.es;

import com.google.common.collect.ImmutableList;
import com.netflix.ndbench.api.plugin.NdBenchMonitor;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class AutotuneTest extends AbstractPluginTest {
    private static final Logger logger = LoggerFactory.getLogger(AutotuneTest.class);
	
    @Test
    public void testRateStopsIncreasingAfterAcceptableWriteFailureThresholdReached() throws Exception {

        NdBenchMonitor noWritesYet = getMonitorInstance(0, 0);
        NdBenchMonitor failuresAlmostAtThreshold = getMonitorInstance(101, 1);
        NdBenchMonitor failuresExactlyAtThreshold = getMonitorInstance(100, 1);
        NdBenchMonitor failuresSlightlyOverThreshold = getMonitorInstance(99, 1);


        List<Double> result1 =
                stepThroughRateIncreases(noWritesYet, noWritesYet, noWritesYet);
        logger.info("result1:" + result1);
        assert (result1.equals(ImmutableList.of(1.0, 2.0, 3.0, 11.0)));


        List<Double> result2 =
                stepThroughRateIncreases(noWritesYet, failuresAlmostAtThreshold, failuresAlmostAtThreshold);
        logger.info("result2:" + result2);
        assert (result2.equals(ImmutableList.of(1.0, 2.0, 3.0, 11.0)));


        List<Double> result3 =
                stepThroughRateIncreases(noWritesYet, failuresExactlyAtThreshold, failuresExactlyAtThreshold);
        logger.info("result3:" + result3);
        assert (result3.equals(ImmutableList.of(1.0, 0.0, 0.0, 1.0)));  // expect passed in current rate (0) after 2nd one..


        List<Double> result4 =
                stepThroughRateIncreases(noWritesYet, failuresSlightlyOverThreshold, failuresSlightlyOverThreshold);
        logger.info("result4:" + result4);
        assert (result4.equals(ImmutableList.of(1.0, 0.0, 0.0, 1.0)));  // expect passed in current rate (0) after 2nd one..


        // This next test verifies that when the failure rate rises above allowed threshold we stop increasing. However,
        // if at some future time enough successful writes go through to the extent that the rate subsequently
        // drops below that threshold, we will once again start increasing the rate (if we have not yet reached the
        // max rate.)
        List<Double> result5 =
                stepThroughRateIncreases(noWritesYet, failuresSlightlyOverThreshold, failuresAlmostAtThreshold);
        logger.info("result5:" + result5);
        assert (result5.equals(ImmutableList.of(1.0, 0.0, 1.0, 11.0)));  // expect passed in current rate (0) after 2nd one..
    }

    private List<Double> stepThroughRateIncreases(NdBenchMonitor m1,
                                                  NdBenchMonitor m2,
                                                  NdBenchMonitor m3) throws Exception {
        IEsConfig config =
                getConfig(9200, "google.com", "junkIndexName", true, 0.01f, 0);
        EsRestPlugin plugin =
                new EsRestPlugin(
                        getCoreConfig(1, true, 100, 10, 11, 0.01f),
                        config,
                        new MockServiceDiscoverer(9200),
                        false);
        plugin.init(null);
        WriteResult okWriteResult = new WriteResult(0);


        NdBenchMonitor failuresAlmostAtThreshold = getMonitorInstance(101, 1);


        List<Double> result;

        ArrayList<Double> returnValue = new ArrayList<Double>();

        Double res1 = plugin.autoTuneWriteRateLimit(0D, Collections.singletonList(okWriteResult), m1);      // assume current rate is zero
        returnValue.add(res1);
        Thread.sleep(11);

        Double res2 = plugin.autoTuneWriteRateLimit(0D, Collections.singletonList(okWriteResult), m2);      // assume current rate is zero
        returnValue.add(res2);
        Thread.sleep(11);

        Double res3 = plugin.autoTuneWriteRateLimit(0D, Collections.singletonList(okWriteResult), m3);      // assume current rate is zero
        returnValue.add(res3);
        Thread.sleep(100);

        Double res4 = plugin.autoTuneWriteRateLimit(0D, Collections.singletonList(okWriteResult), failuresAlmostAtThreshold);
        returnValue.add(res4);

        result = returnValue;
        return result;
    }


    NdBenchMonitor getMonitorInstance(final int writeSuccesses, final int writeFailures) {
        return new NdBenchMonitor() {

            @Override
            public void initialize() {

            }

            @Override
            public void incReadSuccess() {

            }

            @Override
            public long getReadSuccess() {
                return 0;
            }

            @Override
            public void incReadFailure() {

            }

            @Override
            public long getReadFailure() {
                return 0;
            }

            @Override
            public void incWriteSuccess() {

            }

            @Override
            public long getWriteSuccess() {
                return writeSuccesses;
            }

            @Override
            public void incWriteFailure() {

            }

            @Override
            public long getWriteFailure() {
                return writeFailures;
            }

            @Override
            public void incCacheHit() {

            }

            @Override
            public long getCacheHits() {
                return 0;
            }

            @Override
            public void incCacheMiss() {

            }

            @Override
            public long getCacheMiss() {
                return 0;
            }

            @Override
            public void recordReadLatency(long duration) {

            }

            @Override
            public long getReadLatAvg() {
                return 0;
            }

            @Override
            public long getReadLatP50() {
                return 0;
            }

            @Override
            public long getReadLatP95() {
                return 0;
            }

            @Override
            public long getReadLatP99() {
                return 0;
            }

            @Override
            public long getReadLatP995() {
                return 0;
            }

            @Override
            public long getReadLatP999() {
                return 0;
            }

            @Override
            public long getWriteLatAvg() {
                return 0;
            }

            @Override
            public long getWriteLatP50() {
                return 0;
            }

            @Override
            public long getWriteLatP95() {
                return 0;
            }

            @Override
            public long getWriteLatP99() {
                return 0;
            }

            @Override
            public long getWriteLatP995() {
                return 0;
            }

            @Override
            public long getWriteLatP999() {
                return 0;
            }

            @Override
            public long getWriteRPS() {
                return 0;
            }

            @Override
            public long getReadRPS() {
                return 0;
            }

            @Override
            public void setWriteRPS(long writeRPS) {

            }

            @Override
            public void setReadRPS(long readRPS) {

            }

            @Override
            public void recordWriteLatency(long duration) {

            }

            @Override
            public int getCacheHitRatioInt() {
                return 0;
            }

            @Override
            public void resetStats() {

            }
        };
    }
}
