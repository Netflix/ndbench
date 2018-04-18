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
import java.util.function.Function;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.netflix.ndbench.plugin.dynamodb.operations.controlplane.CreateDynamoDBTable;
import com.netflix.ndbench.plugin.dynamodb.operations.controlplane.DeleteDynamoDBTable;
import com.netflix.ndbench.plugin.dynamodb.operations.dataplane.DynamoDBReadBulk;
import com.netflix.ndbench.plugin.dynamodb.operations.dataplane.DynamoDBReadSingle;
import com.netflix.ndbench.plugin.dynamodb.operations.dataplane.DynamoDBWriteBulk;
import com.netflix.ndbench.plugin.dynamodb.operations.dataplane.DynamoDBWriteSingle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.dax.client.dynamodbv2.AmazonDaxClientBuilder;
import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
import com.netflix.ndbench.plugin.dynamodb.configs.DynamoDBConfigs;

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

    @Inject
    private AWSCredentialsProvider awsCredentialsProvider;
    @Inject
    private DynamoDBConfigs config;

    // dynamically initialized
    private AmazonDynamoDB dynamoDB;
    private Function<String, String> singleRead;
    private Function<List<String>, List<String>> bulkRead;
    private Function<String, String> singleWrite;
    private Function<List<String>, List<String>> bulkWrite;
    private CreateDynamoDBTable createTable;
    private DeleteDynamoDBTable deleteTable;

    @Override
    public void init(DataGenerator dataGenerator) {
        logger.info("Initializing DynamoDBKeyValue plugin");
        AmazonDynamoDBClientBuilder builder = AmazonDynamoDBClientBuilder.standard();
        builder.withClientConfiguration(new ClientConfiguration()
                .withMaxConnections(config.getMaxConnections())
                .withRequestTimeout(config.getMaxRequestTimeout()) //milliseconds
                .withRetryPolicy(config.getMaxRetries() <= 0 ? NO_RETRY_POLICY : new RetryPolicy(DEFAULT_RETRY_CONDITION,
                        DYNAMODB_DEFAULT_BACKOFF_STRATEGY,
                        config.getMaxRetries(),
                        DO_HONOR_MAX_ERROR_RETRY_IN_CLIENT_CONFIG))
                .withGzip(config.isCompressing()));
        builder.withCredentials(awsCredentialsProvider);
        if (!Strings.isNullOrEmpty(this.config.getEndpoint())) {
            Preconditions.checkState(!Strings.isNullOrEmpty(config.getRegion()),
                    "If you set the endpoint you must set the region");
            builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(config.getEndpoint(), config.getRegion()));
        }

        if (this.config.isDax()) {
            Preconditions.checkState(!config.programmableTables()); //cluster and table must be created beforehand
            logger.info("Using DAX");
            AmazonDaxClientBuilder amazonDaxClientBuilder = AmazonDaxClientBuilder.standard();
            amazonDaxClientBuilder.withEndpointConfiguration(this.config.getDaxEndpoint());
            dynamoDB = amazonDaxClientBuilder.build();
        } else {
            dynamoDB = builder.build();
        }

        //instantiate operations
        String tableName = config.getTableName();
        String partitionKeyName = config.getAttributeName();

        //control plane
        this.createTable = new CreateDynamoDBTable(dynamoDB, tableName, partitionKeyName,
                Long.parseLong(config.getReadCapacityUnits()), Long.parseLong(config.getWriteCapacityUnits()));
        this.deleteTable = new DeleteDynamoDBTable(dynamoDB, tableName, partitionKeyName);

        //data plane
        boolean consistentRead = config.consistentRead();
        this.singleRead = new DynamoDBReadSingle(dataGenerator, dynamoDB, tableName, partitionKeyName, consistentRead);
        this.bulkRead = new DynamoDBReadBulk(dataGenerator, dynamoDB, tableName, partitionKeyName, consistentRead);
        this.singleWrite = new DynamoDBWriteSingle(dataGenerator, dynamoDB, tableName, partitionKeyName);
        this.bulkWrite = new DynamoDBWriteBulk(dataGenerator, dynamoDB, tableName, partitionKeyName);

        if (this.config.programmableTables()) {
            logger.info("Creating table programmatically");
            TableDescription td = createTable.get();
            logger.info("Table Description: " + td.toString());
        }

        logger.info("DynamoDB Plugin initialized");
    }

    @Override
    public String readSingle(String key) {
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
