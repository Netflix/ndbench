/*
 *  Copyright 2018 Netflix, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
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
public class DynamoDBConfigurationTest {
    @Inject
    DynamoDBConfiguration configuration;

    @Test
    public void testDefaults() {
        assertEquals("ndbench-table", configuration.getTableName());
        assertEquals("id", configuration.getAttributeName());
        assertEquals("5", configuration.getReadCapacityUnits());
        assertEquals("5", configuration.getWriteCapacityUnits());
        assertFalse(configuration.consistentRead());
        assertFalse(configuration.programmableTables());
        assertFalse(configuration.isDax());
        assertNull(configuration.getDaxEndpoint());
        assertFalse(configuration.isCompressing());
        assertNull(configuration.getRegion());
        assertNull(configuration.getEndpoint());
        assertEquals(Integer.valueOf(50), configuration.getMaxConnections());
        assertEquals(Integer.valueOf(-1), configuration.getMaxRequestTimeout());
        assertEquals(Integer.valueOf(10), configuration.getMaxRetries());
        assertNull(configuration.accessKey());
        assertNull(configuration.secretKey());
    }
}
