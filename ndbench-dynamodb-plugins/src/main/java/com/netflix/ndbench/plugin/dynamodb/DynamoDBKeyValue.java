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
package com.netflix.ndbench.plugin.dynamodb;

import java.util.List;
import java.time.Instant;
import java.util.Date;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Preconditions;

import com.google.common.base.Strings;
import com.netflix.ndbench.plugin.dynamodb.configs.DynamoDBConfiguration;
import com.netflix.ndbench.plugin.dynamodb.operations.controlplane.DescribeLimits;
import com.netflix.ndbench.plugin.dynamodb.operations.cloudwatch.controlplane.PutMetricAlarmOperation;
import com.netflix.ndbench.plugin.dynamodb.operations.cloudwatch.dataplane.PutMetricDataOperation;
import com.netflix.ndbench.plugin.dynamodb.operations.dynamodb.controlplane.CreateDynamoDBTable;
import com.netflix.ndbench.plugin.dynamodb.operations.dynamodb.controlplane.DeleteDynamoDBTable;
import com.netflix.ndbench.plugin.dynamodb.operations.dynamodb.dataplane.DynamoDBReadBulk;
import com.netflix.ndbench.plugin.dynamodb.operations.dynamodb.dataplane.DynamoDBReadSingle;
import com.netflix.ndbench.plugin.dynamodb.operations.dynamodb.dataplane.DynamoDBWriteBulk;
import com.netflix.ndbench.plugin.dynamodb.operations.dynamodb.dataplane.DynamoDBWriteSingle;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;

import software.amazon.awssdk.core.auth.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.builder.ClientHttpConfiguration;
import software.amazon.awssdk.core.config.AdvancedClientOption;
import software.amazon.awssdk.core.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.regions.Region;
import software.amazon.awssdk.http.apache.ApacheSdkHttpClientFactory;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClientBuilder;
import software.amazon.awssdk.services.cloudwatch.model.ComparisonOperator;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricAlarmRequest;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.awssdk.services.cloudwatch.model.Statistic;
import software.amazon.awssdk.services.dynamodb.DynamoDBClient;
import software.amazon.awssdk.services.dynamodb.DynamoDBClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.DescribeLimitsResponse;
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

/**
 * This NDBench plugin provides a single key value for AWS DynamoDB.
 * 
 * @author ipapapa
 * @author Alexander Patrikalakis
 */
@Singleton
@NdBenchClientPlugin("DynamoDBKeyValue")
public class DynamoDBKeyValue implements NdBenchClient {
    private static final Logger logger = LoggerFactory.getLogger(DynamoDBKeyValue.class);

    private static final boolean DO_HONOR_MAX_ERROR_RETRY_IN_CLIENT_CONFIG = true;
    private static final String ND_BENCH_DYNAMO_DB_CONSUMED_RCU = "ConsumedRcuHighRes";
    private static final String ND_BENCH_DYNAMO_DB_CONSUMED_WCU = "ConsumedWcuHighRes";
    private static final String CUSTOM_TABLE_METRICS_NAMESPACE = "ndbench/DynamoDB";

    @Inject
    private AwsCredentialsProvider awsCredentialsProvider;
    @Inject
    private DynamoDBConfiguration config;
    @Inject
    private DynamoDBAutoscalingConfigurer dynamoDBAutoscalingConfigurer;

    // dynamically initialized
    private DynamoDBClient dynamoDB;
    private CloudWatchClient cloudWatch;
    private DynamoDBReadSingle singleRead;
    private DynamoDBReadBulk bulkRead;
    private DynamoDBWriteSingle singleWrite;
    private DynamoDBWriteBulk bulkWrite;
    private CreateDynamoDBTable createTable;
    private DeleteDynamoDBTable deleteTable;
    private DescribeLimits describeLimits;
    private PutMetricDataOperation putMetricData;
    private PutMetricAlarmOperation putMetricAlarm;

    private final AtomicReference<ExecutorService> cloudwatchReporterExecutor = new AtomicReference<>(null);

    private String tableName;
    private long publishingInterval;
    private Dimension tableDimension;

    @Override
    public void init(DataGenerator dataGenerator) throws Exception {
        this.tableName = config.getTableName();
        this.publishingInterval = config.getHighResolutionMetricsPublishingInterval();
        final ReturnConsumedCapacity returnConsumedCapacity;
        if (config.publishHighResolutionConsumptionMetrics()) {
            returnConsumedCapacity = ReturnConsumedCapacity.TOTAL;
        } else {
            returnConsumedCapacity = ReturnConsumedCapacity.NONE;
        }
        tableDimension = Dimension.builder().name("TableName").value(tableName).build();
        logger.info("Initializing AWS SDK clients");

        logger.info("Initing DynamoDBKeyValue plugin");
        DynamoDBClientBuilder builder = DynamoDBClient.builder()
                .credentialsProvider(awsCredentialsProvider);
        //TODO figure out how to set request timeout and retry policy in AWS SDK 2.0
        builder.httpConfiguration(
                ClientHttpConfiguration.builder()
                        .httpClient(ApacheSdkHttpClientFactory.builder()
                                .maxConnections(config.getMaxConnections())
                                .build().createHttpClient())
                        .build());

        if (!Strings.isNullOrEmpty(this.config.getEndpoint())) {
            Preconditions.checkState(!Strings.isNullOrEmpty(config.getRegion()),
                    "If you set the endpoint you must set the region");
            builder.endpointOverride(new URI(config.getEndpoint()));
            builder.overrideConfiguration(ClientOverrideConfiguration.builder().advancedOption(AdvancedClientOption.AWS_REGION, Region.of(config.getRegion())).build());
        }
        dynamoDB = builder.build();

        //instantiate operations
        String tableName = config.getTableName();
        String partitionKeyName = config.getAttributeName();
        Preconditions.checkState(StringUtils.isNotEmpty(tableName));
        Preconditions.checkState(StringUtils.isNotEmpty(partitionKeyName));
        long rcu = Long.parseLong(config.getReadCapacityUnits());
        long wcu = Long.parseLong(config.getWriteCapacityUnits());

        //control plane
        this.describeLimits = new DescribeLimits(dynamoDB, tableName, partitionKeyName);
        this.createTable = new CreateDynamoDBTable(dynamoDB, tableName, partitionKeyName, rcu, wcu);
        this.deleteTable = new DeleteDynamoDBTable(dynamoDB, tableName, partitionKeyName);

        //data plane
        boolean consistentRead = config.consistentRead();
        this.singleRead = new DynamoDBReadSingle(dataGenerator, dynamoDB, tableName, partitionKeyName, consistentRead,
                returnConsumedCapacity);
        this.bulkRead = new DynamoDBReadBulk(dataGenerator, dynamoDB, tableName, partitionKeyName, consistentRead,
                returnConsumedCapacity);
        this.singleWrite = new DynamoDBWriteSingle(dataGenerator, dynamoDB, tableName, partitionKeyName,
                returnConsumedCapacity);
        this.bulkWrite = new DynamoDBWriteBulk(dataGenerator, dynamoDB, tableName, partitionKeyName,
                returnConsumedCapacity);

        if (this.config.programmableTables()) {
            logger.info("Creating table programmatically");
            TableDescription td = createTable.get();
            logger.info("Table Description: " + td.toString());
            final DescribeLimitsResponse limits = describeLimits.get();
            final Integer targetWriteUtilization = Integer.parseInt(config.getTargetWriteUtilization());
            final Integer targetReadUtilization = Integer.parseInt(config.getTargetReadUtilization());
            dynamoDBAutoscalingConfigurer.setupAutoscaling(rcu, wcu, config.getTableName(), limits,
                    targetWriteUtilization, targetReadUtilization);
        }

        // build cloudwatch client
        CloudWatchClientBuilder cloudWatchClientBuilder = CloudWatchClient.builder().credentialsProvider(awsCredentialsProvider);
        if (StringUtils.isNotEmpty(this.config.getRegion())) {
            cloudWatchClientBuilder.region(Region.of(config.getRegion()));
        }
        cloudWatch = cloudWatchClientBuilder.build();
        putMetricAlarm = new PutMetricAlarmOperation(cloudWatch);
        putMetricData = new PutMetricDataOperation(cloudWatch);

        if (config.publishHighResolutionConsumptionMetrics()) {
            logger.info("Initializing CloudWatch reporter");
            checkAndInitCloudwatchReporter();
        }

        if (config.alarmOnHighResolutionConsumptionMetrics()) {
            Preconditions.checkState(config.publishHighResolutionConsumptionMetrics());
            //create a high resolution alarm for consuming 80% or more RCU of provisioning
            createHighResolutionAlarm(
                    "ndbench/DynamoDB/RcuConsumedAlarm",
                    ND_BENCH_DYNAMO_DB_CONSUMED_RCU,
                    0.8 * Double.valueOf(config.getReadCapacityUnits()));
            //create a high resolution alarm for consuming 80% or more WCU of provisioning
            createHighResolutionAlarm(
                    "ndbench/DynamoDB/WcuConsumedAlarm",
                    ND_BENCH_DYNAMO_DB_CONSUMED_WCU,
                    0.8 * Double.valueOf(config.getWriteCapacityUnits()));
        }

        logger.info("DynamoDB Plugin initialized");
    }

    /**
     * A high-resolution alarm is one that is configured to fire on threshold breaches of high-resolution metrics. See
     * this [announcement](https://aws.amazon.com/about-aws/whats-new/2017/07/amazon-cloudwatch-introduces-high-resolution-custom-metrics-and-alarms/).
     * DynamoDB only publishes 1 minute consumed capacity metrics. By publishing high resolution consumed capacity
     * metrics on the client side, you can react and alarm on spikes in load much quicker.
     * @param alarmName name of the high resolution alarm to create
     * @param metricName name of the metric to alarm on
     * @param threshold threshold at which to alarm on after 5 breaches of the threshold
     */
    private void createHighResolutionAlarm(String alarmName, String metricName, double threshold) {
        putMetricAlarm.apply(PutMetricAlarmRequest.builder()
                .namespace(CUSTOM_TABLE_METRICS_NAMESPACE)
                .dimensions(tableDimension)
                .metricName(metricName)
                .alarmName(alarmName)
                .statistic(Statistic.SUM)
                .unit(StandardUnit.COUNT)
                .comparisonOperator(ComparisonOperator.GREATER_THAN_THRESHOLD)
                .evaluationPeriods(5) //alarm when 5 out of 5 consecutive measurements are high
                .actionsEnabled(false) //TODO add actions in a later PR
                .period(10) //high resolution alarm
                .threshold(10 * threshold).build());
    }

        @Override
        public String readSingle(String key){
            return singleRead.apply(key);
        }

    @Override
    public String writeSingle(String key) {
        return singleWrite.apply(key);
    }

    @Override
    public List<String> readBulk(List<String> keys) {
        return bulkRead.apply(keys);
    }

    @Override
    public List<String> writeBulk(List<String> keys) {
        return bulkWrite.apply(keys);
    }


    @Override
    public void shutdown() {
        if (this.config.programmableTables()) {
            deleteTable.delete();
        }
        dynamoDB.close();
        logger.info("DynamoDB shutdown");

        if (cloudwatchReporterExecutor.get() != null) {
            cloudwatchReporterExecutor.get().shutdownNow();
            cloudwatchReporterExecutor.set(null);
        }
        cloudWatch.close();
        logger.info("CloudWatch shutdown");
    }

    /*
     * Not needed for this plugin
     * 
     * @see com.netflix.ndbench.api.plugin.NdBenchClient#getConnectionInfo()
     */
    @Override
    public String getConnectionInfo() {
        return String.format("Table Name - %s : Attribute Name - %s : Consistent Read - %b",
                this.config.getTableName(),
                this.config.getAttributeName(),
                this.config.consistentRead());
    }

    @Override
    public String runWorkFlow() {
        return null;
    }

    private void checkAndInitCloudwatchReporter() {
        /** CODE TO PERIODICALLY report high resolution metrics to CloudWatch */
        ExecutorService timer = cloudwatchReporterExecutor.get();
        if (timer == null) {
            timer = Executors.newFixedThreadPool(1);
            timer.submit((Callable<Void>) () -> {
                while (!Thread.currentThread().isInterrupted()) {
                    final Instant now = Instant.now();
                    putMetricData.apply(PutMetricDataRequest.builder()
                            .namespace(CUSTOM_TABLE_METRICS_NAMESPACE)
                            .metricData(createConsumedRcuDatum(now), createConsumedWcuDatum(now))
                            .build());
                    Thread.sleep(publishingInterval);
                }
                return null;
            });
        }
        cloudwatchReporterExecutor.set(timer);
    }

    private MetricDatum createConsumedRcuDatum(Instant now) {
        return createCapacityUnitMetricDatumAndResetCounter(now,
                singleRead.getAndResetConsumed() + bulkRead.getAndResetConsumed(),
                ND_BENCH_DYNAMO_DB_CONSUMED_RCU);
    }

    private MetricDatum createConsumedWcuDatum(Instant now) {
        return createCapacityUnitMetricDatumAndResetCounter(now,
                singleWrite.getAndResetConsumed() + bulkWrite.getAndResetConsumed(),
                ND_BENCH_DYNAMO_DB_CONSUMED_WCU);
    }

    private MetricDatum createCapacityUnitMetricDatumAndResetCounter(Instant now, double count, String name) {
        return MetricDatum.builder()
            .dimensions(tableDimension)
            .metricName(name)
            .storageResolution(1)
            .unit(StandardUnit.COUNT)
            .timestamp(now)
            .value(count)
            .build();
    }
}
