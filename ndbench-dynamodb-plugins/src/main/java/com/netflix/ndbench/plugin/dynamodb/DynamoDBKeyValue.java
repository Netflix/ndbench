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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.retry.RetryPolicy;

import com.amazonaws.services.dynamodbv2.model.DescribeLimitsResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.google.common.base.Preconditions;

import com.netflix.ndbench.plugin.dynamodb.configs.DynamoDBConfiguration;
import com.netflix.ndbench.plugin.dynamodb.operations.controlplane.DescribeLimits;

import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.cloudwatch.model.ComparisonOperator;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricAlarmRequest;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.cloudwatch.model.Statistic;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.netflix.ndbench.plugin.dynamodb.operations.cloudwatch.controlplane.PutMetricAlarmOperation;
import com.netflix.ndbench.plugin.dynamodb.operations.cloudwatch.dataplane.PutMetricDataOperation;
import com.netflix.ndbench.plugin.dynamodb.operations.controlplane.DescribeLimits;
import com.netflix.ndbench.plugin.dynamodb.operations.dynamodb.controlplane.CreateDynamoDBTable;
import com.netflix.ndbench.plugin.dynamodb.operations.dynamodb.controlplane.DeleteDynamoDBTable;
import com.netflix.ndbench.plugin.dynamodb.operations.dynamodb.dataplane.DynamoDBReadBulk;
import com.netflix.ndbench.plugin.dynamodb.operations.dynamodb.dataplane.DynamoDBReadSingle;
import com.netflix.ndbench.plugin.dynamodb.operations.dynamodb.dataplane.DynamoDBWriteBulk;
import com.netflix.ndbench.plugin.dynamodb.operations.dynamodb.dataplane.DynamoDBWriteSingle;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.dax.client.dynamodbv2.AmazonDaxClientBuilder;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;

import static com.amazonaws.retry.PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION;
import static com.amazonaws.retry.PredefinedRetryPolicies.DYNAMODB_DEFAULT_BACKOFF_STRATEGY;
import static com.amazonaws.retry.PredefinedRetryPolicies.NO_RETRY_POLICY;

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
    private AWSCredentialsProvider awsCredentialsProvider;
    @Inject
    private DynamoDBConfiguration config;
    @Inject
    private DynamoDBAutoscalingConfigurer dynamoDBAutoscalingConfigurer;

    // dynamically initialized
    private AmazonDynamoDB dynamoDB;
    private AmazonCloudWatch cloudWatch;
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
    public void init(DataGenerator dataGenerator) {
        this.tableName = config.getTableName();
        this.publishingInterval = config.getHighResolutionMetricsPublishingInterval();
        final ReturnConsumedCapacity returnConsumedCapacity;
        if (config.publishHighResolutionConsumptionMetrics()) {
            returnConsumedCapacity = ReturnConsumedCapacity.TOTAL;
        } else {
            returnConsumedCapacity = ReturnConsumedCapacity.NONE;
        }
        tableDimension = new Dimension().withName("TableName").withValue(tableName);
        logger.info("Initializing AWS SDK clients");

        // build dynamodb client
        AmazonDynamoDBClientBuilder dynamoDbBuilder = AmazonDynamoDBClientBuilder.standard();
        dynamoDbBuilder.withClientConfiguration(new ClientConfiguration()
                .withMaxConnections(config.getMaxConnections())
                .withRequestTimeout(config.getMaxRequestTimeout()) //milliseconds
                .withRetryPolicy(config.getMaxRetries() <= 0 ? NO_RETRY_POLICY : new RetryPolicy(DEFAULT_RETRY_CONDITION,
                        DYNAMODB_DEFAULT_BACKOFF_STRATEGY,
                        config.getMaxRetries(),
                        DO_HONOR_MAX_ERROR_RETRY_IN_CLIENT_CONFIG))
                .withGzip(config.isCompressing()));
        dynamoDbBuilder.withCredentials(awsCredentialsProvider);
        if (StringUtils.isNotEmpty(this.config.getEndpoint())) {
            Preconditions.checkState(StringUtils.isNotEmpty(config.getRegion()),
                    "If you set the endpoint you must set the region");
            dynamoDbBuilder.withEndpointConfiguration(
                    new AwsClientBuilder.EndpointConfiguration(config.getEndpoint(), config.getRegion()));
        }

        if (this.config.isDax()) {
            Preconditions.checkState(!config.programmableTables()); //cluster and table must be created beforehand
            logger.info("Using DAX");
            AmazonDaxClientBuilder amazonDaxClientBuilder = AmazonDaxClientBuilder.standard();
            amazonDaxClientBuilder.withEndpointConfiguration(this.config.getDaxEndpoint());
            dynamoDB = amazonDaxClientBuilder.build();
        } else {
            dynamoDB = dynamoDbBuilder.build();
        }

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
            final DescribeLimitsResult limits = describeLimits.get();
            final Integer targetWriteUtilization = Integer.parseInt(config.getTargetWriteUtilization());
            final Integer targetReadUtilization = Integer.parseInt(config.getTargetReadUtilization());
            dynamoDBAutoscalingConfigurer.setupAutoscaling(rcu, wcu, config.getTableName(), limits,
                    targetWriteUtilization, targetReadUtilization);
        }

        // build cloudwatch client
        AmazonCloudWatchClientBuilder cloudWatchClientBuilder = AmazonCloudWatchClientBuilder.standard();
        cloudWatchClientBuilder.withCredentials(awsCredentialsProvider);
        if (StringUtils.isNotEmpty(this.config.getRegion())) {
            cloudWatchClientBuilder.withRegion(Regions.fromName(config.getRegion()));
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
        putMetricAlarm.apply(new PutMetricAlarmRequest()
                .withNamespace(CUSTOM_TABLE_METRICS_NAMESPACE)
                .withDimensions(tableDimension)
                .withMetricName(metricName)
                .withAlarmName(alarmName)
                .withStatistic(Statistic.Sum)
                .withUnit(StandardUnit.Count)
                .withComparisonOperator(ComparisonOperator.GreaterThanThreshold)
                .withDatapointsToAlarm(5).withEvaluationPeriods(5) //alarm when 5 out of 5 consecutive measurements are high
                .withActionsEnabled(false) //TODO add actions in a later PR
                .withPeriod(10) //high resolution alarm
                .withThreshold(10 * threshold));
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
        dynamoDB.shutdown();
        logger.info("DynamoDB shutdown");

        if (cloudwatchReporterExecutor.get() != null) {
            cloudwatchReporterExecutor.get().shutdownNow();
            cloudwatchReporterExecutor.set(null);
        }
        cloudWatch.shutdown();
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
                    final Date now = Date.from(Instant.now());
                    putMetricData.apply(new PutMetricDataRequest()
                            .withNamespace(CUSTOM_TABLE_METRICS_NAMESPACE)
                            .withMetricData(createConsumedRcuDatum(now), createConsumedWcuDatum(now)));
                    Thread.sleep(publishingInterval);
                }
                return null;
            });
        }
        cloudwatchReporterExecutor.set(timer);
    }

    private MetricDatum createConsumedRcuDatum(Date now) {
        return createCapacityUnitMetricDatumAndResetCounter(now,
                singleRead.getAndResetConsumed() + bulkRead.getAndResetConsumed(),
                ND_BENCH_DYNAMO_DB_CONSUMED_RCU);
    }

    private MetricDatum createConsumedWcuDatum(Date now) {
        return createCapacityUnitMetricDatumAndResetCounter(now,
                singleWrite.getAndResetConsumed() + bulkWrite.getAndResetConsumed(),
                ND_BENCH_DYNAMO_DB_CONSUMED_WCU);
    }

    private MetricDatum createCapacityUnitMetricDatumAndResetCounter(Date now, double count, String name) {
        return new MetricDatum()
            .withDimensions(tableDimension)
            .withMetricName(name)
            .withStorageResolution(1)
            .withUnit(StandardUnit.Count)
            .withTimestamp(now)
            .withValue(count);
    }
}
