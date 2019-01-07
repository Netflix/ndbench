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

import com.netflix.archaius.api.annotations.DefaultValue;

/**
 * Configurations for DynamoDB benchmarks
 *
 * @author ipapapa
 * @author Alexander Patrikalakis
 */
public interface DynamoDBConfigurationBase {
    /**
     * The name of the table to use
     */
    @DefaultValue("ndbench-table")
    String getTableName();

    /*
     * Attributes – Each item is composed of one or more attributes. An attribute is
     * a fundamental data element, something that does not need to be broken down
     * any further.
     */
    @DefaultValue("id")
    String getAttributeName();

    /*
     * Consistency: When you request a strongly consistent read, DynamoDB returns a
     * response with the most up-to-date data, reflecting the updates from all prior
     * write operations that were successful.
     */
    @DefaultValue("true")
    Boolean consistentRead();

    /*
     * Compression: HTTP clients for DynamoDB can be configured to use GZip compression.
     * Effects are negligible for small items, but can be significant for large items with
     * high deflation ratios.
     */
    @DefaultValue("false")
    Boolean isCompressing();

    /*
     * Region: Allowing customers to override the region enables baselining cross-region use cases
     */
    String getRegion();

    /*
     * Region: Allowing customers to override the endpoint enables baselining cross-region use cases
     * and testing with DynamoDB local
     */
    String getEndpoint();

    /*
     * Max connections: the HTTP client in the DynamoDB client has a connection pool. Making it configurable here
     * makes it possible to drive workloads from one host that require more than 50 total read and write workers.
     */
    @DefaultValue("50")
    Integer getMaxConnections();

    /*
     * Max client timeout (milliseconds): maximum amount of time HTTP client will wait for a response from DynamoDB.
     * The default -1 means that there is no request timeout by default.
     */
    @DefaultValue("-1")
    Integer getMaxRequestTimeout();

    /*
     * Max SDK retries: maximum number of times the SDK client will retry a request after a retriable exception.
     */
    @DefaultValue("10")
    Integer getMaxRetries();

    /*
     * Number of main table columns and consequently number of child tables - related to use cases of domain and mapping tables.
     */
    @DefaultValue("5")
    Integer getMainTableColsPerRow();

    /*
     * Prefix for child table name
     */
    @DefaultValue("child")
    String getChildTableNamePrefix();
}
