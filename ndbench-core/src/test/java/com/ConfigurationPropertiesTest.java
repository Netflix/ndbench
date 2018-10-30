package com;

import com.netflix.archaius.guice.ArchaiusModule;
import com.netflix.archaius.test.TestPropertyOverride;
import com.netflix.governator.guice.test.ModulesForTesting;
import com.netflix.governator.guice.test.junit4.GovernatorJunit4ClassRunner;
import com.netflix.ndbench.core.config.IConfiguration;
import com.netflix.ndbench.core.defaultimpl.NdBenchGuiceModule;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.inject.Inject;


/**
 * Verifies that system properties may be used to set values returned by dynamic  proxies generated from
 * {@link com.netflix.ndbench.core.config.IConfiguration},using the namespace prefix "ndbench.config."
 *
 */
@RunWith(GovernatorJunit4ClassRunner.class)
@ModulesForTesting({NdBenchGuiceModule.class, ArchaiusModule.class})
@TestPropertyOverride(value={"ndbench.config.numKeys=777" })
public class ConfigurationPropertiesTest {

    @Inject
    IConfiguration config;

    @Test
    public void testInvokingProcessMethodOnWriteOperationSetsNewRateLimit() throws Exception {
        Assert.assertEquals(777, config.getNumKeys());
    }
}

