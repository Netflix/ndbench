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

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;
import com.netflix.archaius.api.annotations.PropertyName;
import com.netflix.ndbench.api.plugin.common.NdBenchConstants;


/**
 * Configurations for DynamoDB benchmarks
 *
 * @author ipapapa
 */
@Configuration(prefix =  NdBenchConstants.PROP_NAMESPACE +  "dynamodb")
public interface DynamoDBConfigs {

    @PropertyName(name = "tablename")
    @DefaultValue("ndbench-table")
    String getTableName();

    /*
     * Attributes – Each item is composed of one or more attributes. An attribute is
     * a fundamental data element, something that does not need to be broken down
     * any further.
     */
    @PropertyName(name = "attributename")
    @DefaultValue("id")
    String getAttributeName();

    /*
     * Used for provisioned throughput
     */
    @PropertyName(name = "rcu")
    @DefaultValue("2")
    String getReadCapacityUnits();

    @PropertyName(name = "wcu")
    @DefaultValue("2")
    String getWriteCapacityUnits();

    /*
     * Consistency: When you request a strongly consistent read, DynamoDB returns a
     * response with the most up-to-date data, reflecting the updates from all prior
     * write operations that were successful.
     */
    @PropertyName(name = "consistentread")
    @DefaultValue("false")
    Boolean consistentRead();

    /*
     * This configuration allows to create a table programmatically (through NDBench).
     * In the init phase, we create a table.
     * In the shutdown phase, we delete the table.
     *
     * In a single node case it works fine. In a multi-node deployment, there
     * are a number of race conditions.
     * * The first instance will create the table. It does not matter too much which does it
     * because we create the table only if does not exist.
     * * The first instance will delete the table. It does not matter too much which does it
     * because the table will be eventually deleted. All others will show exceptions.
     *
     */
    @DefaultValue("false")
    Boolean programmableTables();

    /**
     * DAX: In-memory DynamoDB cache configuration
     */

    /*
     * Enable/Disable usage of DAX
     */
    @PropertyName(name = "dax")
    @DefaultValue("false")
    Boolean isDax();

    /*
     * DAX endpoint
     */
    @DefaultValue("xxx:8111")
    String getDaxEndpoint();

    /*
     * Compression: HTTP clients for DynamoDB can be configured to use GZip compression.
     * Effects are negligible for small items, but can be significant for large items with
     * high deflation ratios.
     */
    @PropertyName(name = "compressing")
    @DefaultValue("false")
    Boolean isCompressing();

    /*
     * Region: Allowing customers to override the region enables baselining cross-region use cases
     */
    @DefaultValue("us-east-1")
    String getRegion();

    /*
     * Region: Allowing customers to override the endpoint enables baselining cross-region use cases
     * and testing with DynamoDB local
     */
    @DefaultValue("https://dynamodb.us-east-1.amazonaws.com")
    String getEndpoint();

    /*
     * Max connections: the HTTP client in the DynamoDB client has a connection pool. Making it configurable here
     * makes it possible to drive workloads from one host that require more than 50 total read and write workers.
     */
    @DefaultValue("50")
    Integer getMaxConnections();

    @PropertyName(name = "AWSAccessKey")
    String accessKey();

    @PropertyName(name = "AWSSecretKey")
    String secretKey();
}
