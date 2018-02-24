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
import com.netflix.ndbench.api.plugin.common.NdBenchConstants;


/**
 * Configurations for DynamoDB benchmarks
 *
 * @author ipapapa
 */
@Configuration(prefix =  NdBenchConstants.PROP_NAMESPACE +  "dynamodb")
public interface DynamoDBConfigs {

	@DefaultValue("ndbench-table")
	String getTableName();

	/*
	 * Attributes – Each item is composed of one or more attributes. An attribute is
	 * a fundamental data element, something that does not need to be broken down
	 * any further.
	 */
	@DefaultValue("name")
	String getAttributeName();

	/*
	 * Used for provisioned throughput
	 */
	@DefaultValue("1")
	String getReadCapacityUnits();

	@DefaultValue("1")
	String getWriteCapacityUnits();

	/*
	 * Consistency: When you request a strongly consistent read, DynamoDB returns a
	 * response with the most up-to-date data, reflecting the updates from all prior
	 * write operations that were successful.
	 */
	@DefaultValue("false")
	Boolean consistentRead();
	
}
