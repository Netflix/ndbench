package com.netflix.ndbench.core;

import com.google.common.util.concurrent.RateLimiter;
import com.netflix.archaius.api.inject.RuntimeLayer;
import com.netflix.archaius.guice.ArchaiusModule;
import com.netflix.archaius.test.Archaius2TestConfig;
import com.netflix.governator.guice.test.ModulesForTesting;
import com.netflix.governator.guice.test.junit4.GovernatorJunit4ClassRunner;
import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.api.plugin.NdBenchMonitor;
import com.netflix.ndbench.api.plugin.common.NdBenchConstants;
import com.netflix.ndbench.core.config.IConfiguration;
import com.netflix.ndbench.core.defaultimpl.NdBenchGuiceModule;
import com.netflix.ndbench.core.operations.WriteOperation;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

@RunWith(GovernatorJunit4ClassRunner.class)
@ModulesForTesting({NdBenchGuiceModule.class, ArchaiusModule.class})
public class NdbenchDriverTest {
    @Rule
    @RuntimeLayer
    public Archaius2TestConfig settableConfig = new Archaius2TestConfig();

    @Inject
    IConfiguration config;

    @Inject
    NdBenchMonitor ndBenchMonitor;

    @Inject
    DataGenerator dataGenerator;

    @Test
    public void testInvokingProcessMethodOnWriteOperationSetsNewRateLimit() throws Exception {
        NdBenchClient mockClientPlugin = mock(NdBenchClient.class);
        when(mockClientPlugin.writeSingle(anyString())).thenReturn("foo");
        when(mockClientPlugin.
                autoTuneWriteRateLimit(anyDouble(), Collections.singletonList(anyString()), any(NdBenchMonitor.class))).
                thenReturn(500D);

        NdBenchMonitor  mockMonitor = mock(NdBenchMonitor .class);
        doNothing().when(mockMonitor).recordReadLatency(anyLong());
        doNothing().when(mockMonitor).incWriteSuccess();

        when(mockClientPlugin.writeSingle(anyString())).thenReturn("foo");

        NdBenchDriver driver = new NdBenchDriver(config, ndBenchMonitor, dataGenerator, settableConfig);
        WriteOperation writeOperation = new WriteOperation(mockClientPlugin);

        writeOperation.
                process(driver, mockMonitor, Collections.singletonList("some-key"), new AtomicReference<>(RateLimiter.create(100)), true);

        int rateFromSettableConfig = settableConfig.getInteger(NdBenchConstants.WRITE_RATE_LIMIT_FULL_NAME);


        assertEquals(rateFromSettableConfig , 500D, .001);

        // Next check won't work unless we figure out how to configure Property Listener to kick in during the test run
        //double rateFromDriverRateLimiter = driver.getWriteLimiter().get().getRate();
        //assertEquals(rateFromDriverRateLimiter, 500D, .001);
    }

    @Test
    public void testInvokingProcessMethodOnBulkWriteOperationSetsNewRateLimit() throws Exception {
        NdBenchClient mockClientPlugin = mock(NdBenchClient.class);
        when(mockClientPlugin.writeSingle(anyString())).thenReturn("foo");
        when(mockClientPlugin.
                autoTuneWriteRateLimit(anyDouble(), Collections.singletonList(anyString()), any(NdBenchMonitor.class))).
                thenReturn(500D);

        NdBenchMonitor  mockMonitor = mock(NdBenchMonitor .class);
        doNothing().when(mockMonitor).recordReadLatency(anyLong());
        doNothing().when(mockMonitor).incWriteSuccess();

        when(mockClientPlugin.writeSingle(anyString())).thenReturn("foo");

        NdBenchDriver driver = new NdBenchDriver(config, ndBenchMonitor, dataGenerator, settableConfig);
        WriteOperation writeOperation = new WriteOperation(mockClientPlugin);


        List<String> keys = new ArrayList<>(1000);
        for (int i = 0; i < 1000; i++) {
            keys.add("keys" + i);
        }

        writeOperation.
                process(driver, mockMonitor, keys, new AtomicReference<>(RateLimiter.create(100)), true);

        int rateFromSettableConfig = settableConfig.getInteger(NdBenchConstants.WRITE_RATE_LIMIT_FULL_NAME);


        assertEquals(rateFromSettableConfig , 500D, .001);
    }


}
