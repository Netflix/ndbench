package com.netflix.ndbench.plugin.dynamodb.configs;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

/**
 * Configurations for DynamoDB benchmarks
 *
 * @author ipapapa
 */
@Configuration(prefix = "ndbench.config.dynamodb")
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
	@DefaultValue("1L")
	Long getReadCapacityUnits();

	@DefaultValue("1L")
	Long getWriteCapacityUnits();

	/*
	 * Consistency: When you request a strongly consistent read, DynamoDB returns a
	 * response with the most up-to-date data, reflecting the updates from all prior
	 * write operations that were successful.
	 */
	@DefaultValue("false")
	Boolean consistentRead();
	
}
