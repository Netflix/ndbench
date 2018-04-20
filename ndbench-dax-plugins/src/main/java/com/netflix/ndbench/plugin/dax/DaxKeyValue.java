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
package com.netflix.ndbench.plugin.dax;

import com.amazon.dax.client.dynamodbv2.AmazonDaxClientBuilder;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
import com.netflix.ndbench.plugin.dax.configs.DaxConfiguration;
import com.netflix.ndbench.plugin.dynamodb.BaseConfigurationDynamoDBKeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@NdBenchClientPlugin("DaxKeyValue")
public class DaxKeyValue extends BaseConfigurationDynamoDBKeyValue<DaxConfiguration> implements NdBenchClient {
    private static final Logger logger = LoggerFactory.getLogger(DaxKeyValue.class);
    private AmazonDynamoDB dynamoDB;

    /**
     * Public constructor to inject credentials and configuration
     *
     * @param awsCredentialsProvider
     * @param configuration
     */
    @Inject
    public DaxKeyValue(AWSCredentialsProvider awsCredentialsProvider, DaxConfiguration configuration) {
        super(awsCredentialsProvider, configuration);
    }

    @Override
    protected void createAndSetDynamoDBClient() {
        AmazonDaxClientBuilder amazonDaxClientBuilder = AmazonDaxClientBuilder.standard();
        amazonDaxClientBuilder.withEndpointConfiguration(this.config.getDaxEndpoint());
        dynamoDB = amazonDaxClientBuilder.build();
    }

    @Override
    public void shutdown() {
        dynamoDB.shutdown();
        logger.info("Shutdown DynamoDB.");
    }
}
