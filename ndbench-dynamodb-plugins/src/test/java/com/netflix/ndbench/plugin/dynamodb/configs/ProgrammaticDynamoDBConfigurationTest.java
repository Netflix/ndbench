package com.netflix.ndbench.plugin.dynamodb.configs;

import com.netflix.archaius.guice.ArchaiusModule;
import com.netflix.governator.guice.test.ModulesForTesting;
import com.netflix.governator.guice.test.junit4.GovernatorJunit4ClassRunner;
import com.netflix.ndbench.aws.defaultimpl.AwsDefaultsModule;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.inject.Inject;

import static org.junit.Assert.*;

@RunWith(GovernatorJunit4ClassRunner.class)
@ModulesForTesting({DynamoDBModule.class, ArchaiusModule.class, AwsDefaultsModule.class})
public class ProgrammaticDynamoDBConfigurationTest {
    @Inject
    ProgrammaticDynamoDBConfiguration configuration;

    @Test
    public void testDefaults() {
        //test the super type
        assertEquals("ndbench-table", configuration.getTableName());
        assertEquals("id", configuration.getAttributeName());
        assertTrue(configuration.consistentRead());
        assertFalse(configuration.isCompressing());
        assertNull(configuration.getRegion());
        assertNull(configuration.getEndpoint());
        assertEquals(Integer.valueOf(50), configuration.getMaxConnections());
        assertEquals(Integer.valueOf(-1), configuration.getMaxRequestTimeout());
        assertEquals(Integer.valueOf(10), configuration.getMaxRetries());

        //test the declared type
        assertEquals("5", configuration.getReadCapacityUnits());
        assertEquals("5", configuration.getWriteCapacityUnits());
        assertEquals(Boolean.TRUE, configuration.getAutoscaling());
        assertEquals("70", configuration.getTargetReadUtilization());
        assertEquals("70", configuration.getTargetWriteUtilization());
        assertEquals(Boolean.FALSE, configuration.publishHighResolutionConsumptionMetrics());
        assertEquals(Long.valueOf(1000L), configuration.getHighResolutionMetricsPublishingInterval());
        assertEquals(Boolean.FALSE, configuration.alarmOnHighResolutionConsumptionMetrics());
        assertEquals(Double.valueOf(80), configuration.highResolutionAlarmThresholdPercentageOfProvisionedCapacity());
    }
}
