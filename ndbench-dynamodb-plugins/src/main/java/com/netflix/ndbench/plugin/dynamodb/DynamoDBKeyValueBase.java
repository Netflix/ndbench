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

import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.plugin.dynamodb.configs.DynamoDBConfigurationBase;

import com.netflix.ndbench.plugin.dynamodb.operations.CapacityConsumingFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.List;

/**
 * Base class with shared fields among the various implementations of NdBenchClient.
 */
public abstract class DynamoDBKeyValueBase<C extends DynamoDBConfigurationBase,
        SingleReadResultType,
        BulkReadResultType,
        SingleWriteResultType,
        BulkWriteResultType,
        CredentialsProviderType> implements NdBenchClient {
    private static final Logger logger = LoggerFactory.getLogger(DynamoDBKeyValueBase.class);
    protected final CredentialsProviderType awsCredentialsProvider;
    protected final C config;

    // dynamically initialized
    protected CapacityConsumingFunction<SingleReadResultType, String, String> singleRead;
    protected CapacityConsumingFunction<BulkReadResultType, List<String>, List<String>> bulkRead;
    protected CapacityConsumingFunction<SingleWriteResultType, String, String> singleWrite;
    protected CapacityConsumingFunction<BulkWriteResultType, List<String>, List<String>> bulkWrite;

    /**
     * Protected method to inject credentials and configuration
     * @param awsCredentialsProvider
     * @param configuration
     */
    protected DynamoDBKeyValueBase(CredentialsProviderType awsCredentialsProvider,
                                   C configuration) {
        this.awsCredentialsProvider = awsCredentialsProvider;
        this.config = configuration;
    }

    protected abstract void instantiateDataPlaneOperations(DataGenerator dataGenerator);

    protected abstract void createAndSetDynamoDBClient();

    @Override
    public void init(DataGenerator dataGenerator) {
        logger.info("Initializing AWS SDK clients");
        createAndSetDynamoDBClient();
        instantiateDataPlaneOperations(dataGenerator);
        logger.info("DynamoDB Plugin initialized");
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
    public abstract void shutdown();

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

    double getAndResetReadCounsumed() {
        return singleRead.getAndResetConsumed() + bulkRead.getAndResetConsumed();
    }

    double getAndResetWriteCounsumed() {
        return singleWrite.getAndResetConsumed() + bulkWrite.getAndResetConsumed();
    }
}
