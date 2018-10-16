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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.cloudwatch.model.ComparisonOperator;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricAlarmRequest;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.cloudwatch.model.Statistic;
import com.amazonaws.services.dynamodbv2.model.DescribeLimitsResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
import com.netflix.ndbench.plugin.dynamodb.configs.ProgrammaticDynamoDBConfiguration;
import com.netflix.ndbench.plugin.dynamodb.operations.cloudwatch.controlplane.PutMetricAlarmOperation;
import com.netflix.ndbench.plugin.dynamodb.operations.cloudwatch.dataplane.PutMetricDataOperation;
import com.netflix.ndbench.plugin.dynamodb.operations.dynamodb.controlplane.DescribeLimits;
import com.netflix.ndbench.plugin.dynamodb.operations.dynamodb.controlplane.CreateDynamoDBTable;
import com.netflix.ndbench.plugin.dynamodb.operations.dynamodb.controlplane.DeleteDynamoDBTable;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

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
@Singleton
@NdBenchClientPlugin("DynamoDBProgrammaticKeyValue")
public class DynamoDBProgrammaticKeyValue extends DynamoDBKeyValueBase<ProgrammaticDynamoDBConfiguration> implements NdBenchClient<String,String> {
    private static final Logger logger = LoggerFactory.getLogger(DynamoDBProgrammaticKeyValue.class);
    private static final String ND_BENCH_DYNAMO_DB_CONSUMED_RCU = "ConsumedRcuHighRes";
    private static final String ND_BENCH_DYNAMO_DB_CONSUMED_WCU = "ConsumedWcuHighRes";
    private static final String CUSTOM_TABLE_METRICS_NAMESPACE = "ndbench/DynamoDB";

    private final AtomicReference<ExecutorService> cloudwatchReporterExecutor = new AtomicReference<>(null);

    private DynamoDBAutoscalingConfigurer dynamoDBAutoscalingConfigurer;
    private AmazonCloudWatch cloudWatch;
    private CreateDynamoDBTable createTable;
    private DeleteDynamoDBTable deleteTable;
    private DescribeLimits describeLimits;
    private PutMetricDataOperation putMetricData;
    private PutMetricAlarmOperation putMetricAlarm;
    private long publishingInterval;
    private Dimension tableDimension;

    /**
     * Public constructor to inject credentials and configuration
     *
     * @param awsCredentialsProvider
     * @param configuration
     */
    @Inject
    public DynamoDBProgrammaticKeyValue(AWSCredentialsProvider awsCredentialsProvider,
                                        ProgrammaticDynamoDBConfiguration configuration,
                                        DynamoDBAutoscalingConfigurer dynamoDBAutoscalingConfigurer) {
        super(awsCredentialsProvider, configuration);
        this.dynamoDBAutoscalingConfigurer = dynamoDBAutoscalingConfigurer;
    }

    @Override
    public void init(DataGenerator dataGenerator) {
        createAndSetDynamoDBClient();
        instantiateDataPlaneOperations(dataGenerator);

        //prerequisite data from configuration
        String tableName = config.getTableName();
        String partitionKeyName = config.getAttributeName();
        long rcu = Long.parseLong(config.getReadCapacityUnits());
        long wcu = Long.parseLong(config.getWriteCapacityUnits());
        this.publishingInterval = config.getHighResolutionMetricsPublishingInterval();
        this.tableDimension = new Dimension().withName("TableName").withValue(tableName);

        //setup control plane operations
        this.describeLimits = new DescribeLimits(dynamoDB, tableName, partitionKeyName);
        this.createTable = new CreateDynamoDBTable(dynamoDB, tableName, partitionKeyName, rcu, wcu);
        this.deleteTable = new DeleteDynamoDBTable(dynamoDB, tableName, partitionKeyName);
        logger.info("Creating table programmatically");
        TableDescription td = createTable.get();
        logger.info("Table Description: " + td.toString());
        final DescribeLimitsResult limits = describeLimits.get();
        if (config.getAutoscaling()) {
            dynamoDBAutoscalingConfigurer.setupAutoscaling(rcu, wcu, config.getTableName(), limits,
                    Integer.valueOf(config.getTargetWriteUtilization()),
                    Integer.valueOf(config.getTargetReadUtilization()));
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
            double thresholdPercentage = config.highResolutionAlarmThresholdPercentageOfProvisionedCapacity();
            //create a high resolution alarm for consuming 80% or more RCU of provisioning
            createHighResolutionAlarm(
                    "ndbench/DynamoDB/RcuConsumedAlarm",
                    ND_BENCH_DYNAMO_DB_CONSUMED_RCU,
                    thresholdPercentage * (double) rcu);
            //create a high resolution alarm for consuming 80% or more WCU of provisioning
            createHighResolutionAlarm(
                    "ndbench/DynamoDB/WcuConsumedAlarm",
                    ND_BENCH_DYNAMO_DB_CONSUMED_WCU,
                    thresholdPercentage * (double) wcu);
        }
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
        return createCapacityUnitMetricDatumAndResetCounter(now, getAndResetReadCounsumed(),
                ND_BENCH_DYNAMO_DB_CONSUMED_RCU);
    }

    private MetricDatum createConsumedWcuDatum(Date now) {
        return createCapacityUnitMetricDatumAndResetCounter(now, getAndResetWriteCounsumed(),
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

    @Override
    public void shutdown() {
        super.shutdown();
        if (cloudwatchReporterExecutor.get() != null) {
            cloudwatchReporterExecutor.get().shutdownNow();
            cloudwatchReporterExecutor.set(null);
        }
        cloudWatch.shutdown();
        logger.info("CloudWatch shutdown");
        deleteTable.delete();
    }
}
