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
 * @author Alexander Patrikalakis
 */
@Configuration(prefix =  NdBenchConstants.PROP_NAMESPACE +  "dynamodb")
public interface ProgrammaticDynamoDBConfiguration extends DynamoDBConfigurationBase {
    /*
     * Provisioned read capacity units to create the table with.
     */
    @DefaultValue("5")
    String getReadCapacityUnits();

    /**
     * Provisioned write capacity units to create the table with.
     */
    @DefaultValue("5")
    String getWriteCapacityUnits();

    /*
     * Application Autoscaling for DynamoDB
     */
    @DefaultValue("true")
    Boolean getAutoscaling();

    /**
     * Target read utilization represented as a percentage of the provisioned read throughput.
     */
    @DefaultValue("70")
    String getTargetReadUtilization();

    /**
     * Target write utilization represented as a percentage of the provisioned write throughput.
     */
    @DefaultValue("70")
    String getTargetWriteUtilization();

    /*
     * DynamoDB publishes one minute metrics for Consumed Capacity. To supplement this metric,
     * ndbench can publish 1-second high resolution metrics of consumed capacity to CloudWatch.
     */
    @DefaultValue("false")
    Boolean publishHighResolutionConsumptionMetrics();

    /*
     * The interval, in milliseconds at which ndbench publishes high resolution consumption metrics to CloudWatch.
     */
    @DefaultValue("1000")
    Long getHighResolutionMetricsPublishingInterval();

    /*
     * DynamoDB publishes one minute metrics for Consumed Capacity. To supplement this metric,
     * ndbench can publish 1-second high resolution metrics of consumed capacity to CloudWatch.
     */
    @DefaultValue("false")
    Boolean alarmOnHighResolutionConsumptionMetrics();

    /*
     * High resolution alarm threshold percentage of consumed capacity.
     */
    @DefaultValue("80")
    Double highResolutionAlarmThresholdPercentageOfProvisionedCapacity();
}
