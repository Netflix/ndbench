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
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
import com.netflix.ndbench.plugin.dynamodb.DynamoDBKeyValueBase;
import com.netflix.ndbench.plugin.dax.configs.DaxConfiguration;
import com.netflix.ndbench.plugin.dynamodb.operations.dynamodb.dataplane.DynamoDBReadBulk;
import com.netflix.ndbench.plugin.dynamodb.operations.dynamodb.dataplane.DynamoDBReadSingle;
import com.netflix.ndbench.plugin.dynamodb.operations.dynamodb.dataplane.DynamoDBWriteBulk;
import com.netflix.ndbench.plugin.dynamodb.operations.dynamodb.dataplane.DynamoDBWriteSingle;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@NdBenchClientPlugin("DaxKeyValue")
public class DaxKeyValue extends DynamoDBKeyValueBase<DaxConfiguration> implements NdBenchClient {
    private static final Logger logger = LoggerFactory.getLogger(DaxKeyValue.class);

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
    public void init(DataGenerator dataGenerator) {
        logger.info("Initializing AWS DAX client");

        AmazonDaxClientBuilder amazonDaxClientBuilder = AmazonDaxClientBuilder.standard();
        amazonDaxClientBuilder.withEndpointConfiguration(this.config.getDaxEndpoint());
        dynamoDB = amazonDaxClientBuilder.build();

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

        logger.info("DynamoDB Plugin initialized");
    }
}
