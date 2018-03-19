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
	Boolean programTables();
	
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
	
}
