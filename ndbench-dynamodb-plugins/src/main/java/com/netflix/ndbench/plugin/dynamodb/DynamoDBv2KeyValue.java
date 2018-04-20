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

import java.net.URISyntaxException;

import com.google.common.base.Preconditions;

import com.google.common.base.Strings;
import com.netflix.ndbench.plugin.dynamodb.configs.DynamoDBConfiguration;

import com.netflix.ndbench.plugin.dynamodb.operations.v2.dynamodb.dataplane.DynamoDBReadBulk;
import com.netflix.ndbench.plugin.dynamodb.operations.v2.dynamodb.dataplane.DynamoDBReadSingle;
import com.netflix.ndbench.plugin.dynamodb.operations.v2.dynamodb.dataplane.DynamoDBWriteBulk;
import com.netflix.ndbench.plugin.dynamodb.operations.v2.dynamodb.dataplane.DynamoDBWriteSingle;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.Protocol;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity;

/**
 * This NDBench plugin provides a single key value for AWS DynamoDB.
 *
 * @author ipapapa
 * @author Alexander Patrikalakis
 */
@Singleton
@NdBenchClientPlugin("DynamoDBv2KeyValue")
public class DynamoDBv2KeyValue extends DynamoDBKeyValueBase<DynamoDBConfiguration, GetItemResponse,
        BatchGetItemResponse, PutItemResponse, BatchWriteItemResponse, AwsCredentialsProvider> implements NdBenchClient {
    private static final Logger logger = LoggerFactory.getLogger(DynamoDBv2KeyValue.class);

    // dynamically initialized
    private DynamoDbAsyncClient dynamoDB;

    /**
     * Protected method to inject credentials and configuration
     *
     * @param awsCredentialsProvider
     * @param configuration
     */
    @Inject
    public DynamoDBv2KeyValue(AwsCredentialsProvider awsCredentialsProvider, DynamoDBConfiguration configuration) {
        super(awsCredentialsProvider, configuration);
    }

    @Override
    protected void instantiateDataPlaneOperations(DataGenerator dataGenerator) {
        //instantiate operations
        ReturnConsumedCapacity returnConsumedCapacity = ReturnConsumedCapacity.NONE;
        String tableName = config.getTableName();
        String partitionKeyName = config.getAttributeName();
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
    protected void createAndSetDynamoDBClient() {
        DynamoDbAsyncClientBuilder builder = DynamoDbAsyncClient.builder().credentialsProvider(awsCredentialsProvider);
        //TODO figure out how to set request timeout and retry policy in AWS SDK 2.0
        builder.httpClient(NettyNioAsyncHttpClient.builder()
                        .maxConcurrency(config.getMaxConnections())
                        .protocol(Protocol.HTTP1_1) //TODO evaluate HTTP2
                        .build());

        if (!Strings.isNullOrEmpty(this.config.getEndpoint())) {
            Preconditions.checkState(!Strings.isNullOrEmpty(config.getRegion()),
                    "If you set the endpoint you must set the region");
            try {
                builder.endpointOverride(new URI(config.getEndpoint()));
            } catch (URISyntaxException e) {
                throw new IllegalStateException("Unable to convert endpoint", e);
            }
            builder.region(Region.of(config.getRegion()));
        }
        dynamoDB = builder.build();
    }

    @Override
    public void shutdown() {
        dynamoDB.close();
        logger.info("DynamoDB shutdown");
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
}
