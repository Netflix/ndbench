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
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.plugin.dynamodb.configs.DynamoDBConfigurationBase;
import com.netflix.ndbench.plugin.dynamodb.operations.dynamodb.dataplane.DynamoDBReadBulk;
import com.netflix.ndbench.plugin.dynamodb.operations.dynamodb.dataplane.DynamoDBReadSingle;
import com.netflix.ndbench.plugin.dynamodb.operations.dynamodb.dataplane.DynamoDBWriteBulk;
import com.netflix.ndbench.plugin.dynamodb.operations.dynamodb.dataplane.DynamoDBWriteSingle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Base class with shared fields among the various implementations of NdBenchClient.
 */
public abstract class DynamoDBKeyValueBase<C extends DynamoDBConfigurationBase> implements NdBenchClient {
    private static final Logger logger = LoggerFactory.getLogger(DynamoDBKeyValueBase.class);

    protected final AWSCredentialsProvider awsCredentialsProvider;
    protected final C config;

    // dynamically initialized
    protected AmazonDynamoDB dynamoDB;
    protected DynamoDBReadSingle singleRead;
    protected DynamoDBReadBulk bulkRead;
    protected DynamoDBWriteSingle singleWrite;
    protected DynamoDBWriteBulk bulkWrite;

    /**
     * Protected method to inject credentials and configuration
     * @param awsCredentialsProvider
     * @param configuration
     */
    protected DynamoDBKeyValueBase(AWSCredentialsProvider awsCredentialsProvider,
                                   C configuration) {
        this.awsCredentialsProvider = awsCredentialsProvider;
        this.config = configuration;
    }

    @Override
    public abstract void init(DataGenerator dataGenerator);

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

    public void shutdown() {
        dynamoDB.shutdown();
        logger.info("DynamoDB shutdown");
    }

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
