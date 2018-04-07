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

import com.netflix.ndbench.plugin.dynamodb.configs.DynamoDBConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;

/**
 * This NDBench plugin provides a single key value for AWS DynamoDB.
 * 
 * @author ipapapa
 * @author Alexander Patrikalakis
 */
@Singleton
@NdBenchClientPlugin("DynamoDBKeyValue")
public class DynamoDBKeyValue extends DynamoDBKeyValueBase<DynamoDBConfiguration> implements NdBenchClient {
    private static final Logger logger = LoggerFactory.getLogger(DynamoDBKeyValue.class);

    /**
     * Public constructor to inject credentials and configuration
     *
     * @param awsCredentialsProvider
     * @param configuration
     */
    @Inject
    public DynamoDBKeyValue(AWSCredentialsProvider awsCredentialsProvider, DynamoDBConfiguration configuration) {
        super(awsCredentialsProvider, configuration);
    }

    @Override
    public void init(DataGenerator dataGenerator) {
        logger.info("Initializing AWS SDK clients");

        createAndSetDynamoDBClient();
        instantiateDataPlaneOperations(dataGenerator);

        logger.info("DynamoDB Plugin initialized");
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
