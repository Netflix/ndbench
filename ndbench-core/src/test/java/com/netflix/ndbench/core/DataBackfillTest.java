package com.netflix.ndbench.core;

import java.util.List;
import javax.inject.Inject;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.netflix.archaius.guice.ArchaiusModule;
import com.netflix.governator.guice.test.ModulesForTesting;
import com.netflix.governator.guice.test.junit4.GovernatorJunit4ClassRunner;
import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.core.config.IConfiguration;
import com.netflix.ndbench.core.defaultimpl.NdBenchGuiceModule;

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
    public void backfill() throws Exception
    {
        NdBenchClient mockClientPlugin = mock(NdBenchClient.class);
        when(mockClientPlugin.writeSingle(anyString())).thenReturn("foo");
        dataBackfill.backfill(mockClientPlugin);
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

    @Test
    public void getKeyRangesPerThread()
    {
        for (int i = 0; i < 100; i++)
        {
            List<Pair<Integer, Integer>> s = dataBackfill.getKeyRangesPerThread(10, 4, 100);
            s.forEach(st_end -> Assert.assertTrue(st_end.getRight() <= 100));
            s.forEach(System.out::println);
        }
    }
}