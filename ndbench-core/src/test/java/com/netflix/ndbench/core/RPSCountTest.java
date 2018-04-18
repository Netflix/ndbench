package com.netflix.ndbench.core;

import com.google.common.util.concurrent.RateLimiter;
import com.netflix.ndbench.api.plugin.NdBenchMonitor;
import com.netflix.ndbench.core.config.IConfiguration;
import org.apache.log4j.Level;
import org.junit.Rule;
import org.junit.Test;
import org.libex.test.TestBase;
import org.libex.test.logging.log4j.Log4jCapturer;
import org.libex.test.logging.log4j.Log4jCapturer.LogAssertion;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RPSCountTest extends TestBase {

    @Rule
    public Log4jCapturer logCapturer = Log4jCapturer.builder().build();

    @Test
    public void testMessageLogged() {

        // Note: readSuccess+readFail will be divided by stats update frequency of 10,
        // and similarly for writeSuccess+writeFail
        //
        verifyLoggerActivity(                                   // verify no logging if expected rate < observed rate
                "Observed Read RPS",
                false,
                getRPSCount(
                        true, true, 9/*readRate*/, 1/*writeRate*/,
                        100/*readSuccess*/, 0/*readFail*/,
                        0/*writeSuccess*/, 0/*writeFail*/));
        verifyLoggerActivity(                                   // verify no logging if expected rate == observed rate
                "Observed Read RPS",
                false,
                getRPSCount(
                        true, true, 9/*readRate*/, 1/*writeRate*/,
                        90/*readSuccess*/, 0/*readFail*/,
                        0/*writeSuccess*/, 0/*writeFail*/));
        verifyLoggerActivity(                                   // verify have logging if expected rate > observed rate
                "Observed Read RPS",
                true,
                getRPSCount(
                        true, true, 9/*readRate*/, 1/*writeRate*/,
                        89/*readSuccess*/, 0/*readFail*/,
                        0/*writeSuccess*/, 0/*writeFail*/));
        verifyLoggerActivity(                                   // verify have logging if expected rate > observed rate
                "Observed Read RPS",
                false,
                getRPSCount(
                        false, true, 9/*readRate*/, 1/*writeRate*/,
                        89/*readSuccess*/, 0/*readFail*/,
                        0/*writeSuccess*/, 0/*writeFail*/));

        verifyLoggerActivity(                                   // verify no logging if expected rate < observed rate
                "Observed Write RPS",
                false,
                getRPSCount(
                        true, true, 1/*readRate*/, 9/*writeRate*/,
                        1/*readSuccess*/, 0/*readFail*/,
                        100/*writeSuccess*/, 0/*writeFail*/));
        verifyLoggerActivity(                                   // verify no logging if expected rate == observed rate
                "Observed Write RPS",
                false,
                getRPSCount(
                        true, true, 1/*readRate*/, 9/*writeRate*/,
                        0/*readSuccess*/, 0/*readFail*/,
                        90/*writeSuccess*/, 0/*writeFail*/));
        verifyLoggerActivity(                                   // verify have logging if expected rate > observed rate
                "Observed Write RPS",
                true,
                getRPSCount(
                        true, true, 1/*readRate*/, 9/*writeRate*/,
                        1/*readSuccess*/, 0/*readFail*/,
                        89/*writeSuccess*/, 0/*writeFail*/));
        verifyLoggerActivity(                                   // verify have logging if expected rate > observed rate
                "Observed Write RPS",
                false,
                getRPSCount(
                        true, false, 1/*readRate*/, 9/*writeRate*/,
                        1/*readSuccess*/, 0/*readFail*/,
                        89/*writeSuccess*/, 0/*writeFail*/));
    }

    private void verifyLoggerActivity(String fragmentOfExpectedLoggedMsg,
                                      boolean shouldBeLogged,
                                      RPSCount counter) {
        logCapturer.clearLog();
        counter.updateRPS();

        logCapturer.assertThat(LogAssertion.newLogAssertion()
                .withLevel(Level.DEBUG).isNotLogged());
        LogAssertion assertionTmp = LogAssertion.newLogAssertion()
                .withLevel(Level.WARN).withRenderedMessage(fragmentOfExpectedLoggedMsg);
        LogAssertion assertion;
        if (shouldBeLogged) {
            assertion = assertionTmp.isLogged();
        } else {
            assertion = assertionTmp.isNotLogged();
        }
        logCapturer.assertThat(assertion);
    }

    private RPSCount getRPSCount(boolean readsStarted,
                                 boolean writesStarted,
                                 double readRate,
                                 double writeRate,
                                 long readSuccess,
                                 long readFailure,
                                 long writeSuccess,
                                 long writeFailure) {

        IConfiguration config = mock(IConfiguration.class);
        when(config.getStatsUpdateFreqSeconds()).thenReturn(10);
        when(config.isReadEnabled()).thenReturn(true);
        when(config.isWriteEnabled()).thenReturn(true);


        NdBenchMonitor monitor = mock(NdBenchMonitor.class);
        when(monitor.getReadSuccess()).thenReturn(readSuccess);
        when(monitor.getReadFailure()).thenReturn(readFailure);
        when(monitor.getWriteSuccess()).thenReturn(writeSuccess);
        when(monitor.getWriteFailure()).thenReturn(writeFailure);

        RPSCount counter =
                new RPSCount(
                        new AtomicBoolean(readsStarted),
                        new AtomicBoolean(writesStarted),
                        new AtomicReference(RateLimiter.create(readRate)),
                        new AtomicReference(RateLimiter.create(writeRate)),
                        config,
                        monitor);
        return counter;
    }
}
