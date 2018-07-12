package com.netflix.ndbench.plugin.dynamodb.configs;

import com.netflix.archaius.guice.ArchaiusModule;
import com.netflix.governator.guice.test.ModulesForTesting;
import com.netflix.governator.guice.test.junit4.GovernatorJunit4ClassRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.inject.Inject;

import static org.junit.Assert.*;

@RunWith(GovernatorJunit4ClassRunner.class)
@ModulesForTesting({DynamoDBModule.class, ArchaiusModule.class})
public class DaxConfigurationTest {
    @Inject
    DaxConfiguration configuration;

    @Test
    public void testDefaults() {
        //super type
        assertEquals("ndbench-table", configuration.getTableName());
        assertEquals("id", configuration.getAttributeName());
        assertTrue(configuration.consistentRead());
        assertFalse(configuration.isCompressing());
        assertNull(configuration.getRegion());
        assertNull(configuration.getEndpoint());
        assertEquals(Integer.valueOf(50), configuration.getMaxConnections());
        assertEquals(Integer.valueOf(-1), configuration.getMaxRequestTimeout());
        assertEquals(Integer.valueOf(10), configuration.getMaxRetries());

        //this type
        assertNull(configuration.getDaxEndpoint());
    }
}
