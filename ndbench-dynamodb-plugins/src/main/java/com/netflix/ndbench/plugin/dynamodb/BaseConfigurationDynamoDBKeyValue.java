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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemResult;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.google.common.base.Preconditions;

import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.netflix.ndbench.plugin.dynamodb.configs.DynamoDBConfigurationBase;
import com.netflix.ndbench.plugin.dynamodb.operations.v1.dynamodb.dataplane.DynamoDBReadBulk;
import com.netflix.ndbench.plugin.dynamodb.operations.v1.dynamodb.dataplane.DynamoDBReadSingle;
import com.netflix.ndbench.plugin.dynamodb.operations.v1.dynamodb.dataplane.DynamoDBWriteBulk;
import com.netflix.ndbench.plugin.dynamodb.operations.v1.dynamodb.dataplane.DynamoDBWriteSingle;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchClient;

import static com.amazonaws.retry.PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION;
import static com.amazonaws.retry.PredefinedRetryPolicies.DYNAMODB_DEFAULT_BACKOFF_STRATEGY;
import static com.amazonaws.retry.PredefinedRetryPolicies.NO_RETRY_POLICY;

/**
 * This NDBench plugin provides a single key value for AWS DynamoDB.
 * 
 * @author ipapapa
 * @author Alexander Patrikalakis
 */
public abstract class BaseConfigurationDynamoDBKeyValue<C extends DynamoDBConfigurationBase> extends DynamoDBKeyValueBase<C,
        GetItemResult, BatchGetItemResult, PutItemResult, BatchWriteItemResult, AWSCredentialsProvider> implements NdBenchClient {
    private static final boolean DO_HONOR_MAX_ERROR_RETRY_IN_CLIENT_CONFIG = true;
    private static final Logger logger = LoggerFactory.getLogger(BaseConfigurationDynamoDBKeyValue.class);

    AmazonDynamoDB dynamoDB;

    /**
     * Public constructor to inject credentials and configuration
     *
     * @param awsCredentialsProvider
     * @param configuration
     */

    public BaseConfigurationDynamoDBKeyValue(AWSCredentialsProvider awsCredentialsProvider, C configuration) {
        super(awsCredentialsProvider, configuration);
    }

    protected void createAndSetDynamoDBClient() {
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
        this.dynamoDB = dynamoDbBuilder.build();
    }

    protected void instantiateDataPlaneOperations(DataGenerator dataGenerator) {
        //instantiate operations
        String tableName = config.getTableName();
        String partitionKeyName = config.getAttributeName();
        ReturnConsumedCapacity returnConsumedCapacity = ReturnConsumedCapacity.NONE;
        Preconditions.checkState(StringUtils.isNotEmpty(tableName));
        Preconditions.checkState(StringUtils.isNotEmpty(partitionKeyName));

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
    }

    @Override
    public void shutdown() {
        logger.info("DynamoDB shutdown");
        dynamoDB.shutdown();
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

    AmazonDynamoDB getDynamoDB() {
        return this.dynamoDB;
    }
}
