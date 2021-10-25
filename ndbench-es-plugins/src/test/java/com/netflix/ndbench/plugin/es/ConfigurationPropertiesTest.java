package com.netflix.ndbench.plugin.es;

import com.netflix.archaius.guice.ArchaiusModule;
import com.netflix.governator.guice.test.ModulesForTesting;
import com.netflix.governator.guice.test.junit4.GovernatorJunit4ClassRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.inject.Inject;


/**
 * Verifies that system properties may be used to set values returned by dynamic proxies generated from
 * {@link EsConfig},using the namespace prefix "ndbench.config.es".
 */
@RunWith(GovernatorJunit4ClassRunner.class)
@ModulesForTesting({EsModule.class, ArchaiusModule.class})
public class ConfigurationPropertiesTest {
    static {
        System.setProperty("ndbench.config.es.connectTimeoutSeconds", "777");
    }

    @Inject
    EsConfig esConfig;

    @Test
    public void testInvokingProcessMethodOnWriteOperationSetsNewRateLimit() {
        assert (esConfig.getConnectTimeoutSeconds() == 777);
        assert (esConfig.getConnectionRequestTimeoutSeconds() == 120);
        assert (esConfig.getSocketTimeoutSeconds() == 120);
        assert (esConfig.getMaxRetryTimeoutSeconds() == 120);
    }
}

