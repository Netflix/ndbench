package com.netflix.ndbench.core;

import java.util.Collections;
import javax.inject.Inject;

import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.netflix.archaius.api.inject.RuntimeLayer;
import com.netflix.archaius.guice.ArchaiusModule;
import com.netflix.archaius.test.Archaius2TestConfig;
import com.netflix.governator.guice.test.ModulesForTesting;
import com.netflix.governator.guice.test.junit4.GovernatorJunit4ClassRunner;
import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.api.plugin.NdBenchMonitor;
import com.netflix.ndbench.core.config.IConfiguration;
import com.netflix.ndbench.core.defaultimpl.NdBenchGuiceModule;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyDouble;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@RunWith(GovernatorJunit4ClassRunner.class)
@ModulesForTesting({ NdBenchGuiceModule.class, ArchaiusModule.class})
public class DataBackfillTest
{
    @Inject
    DataBackfill dataBackfill;

    @Inject
    IConfiguration config;

    @After
    public void afterMethod()
    {
        dataBackfill.stopBackfill();
    }


    @Test
    public void backfillAsync() throws Exception
    {
        NdBenchClient mockClientPlugin = mock(NdBenchClient.class);
        when(mockClientPlugin.writeSingle(anyString())).thenReturn("foo");
        dataBackfill.backfillAsync(mockClientPlugin);
    }

    @Test
    public void backfillAsyncRestart() throws Exception
    {
        NdBenchClient mockClientPlugin = mock(NdBenchClient.class);
        when(mockClientPlugin.writeSingle(anyString())).thenReturn("foo");
        dataBackfill.backfillAsync(mockClientPlugin);
        dataBackfill.stopBackfill();
        dataBackfill.backfillAsync(mockClientPlugin);
    }
}