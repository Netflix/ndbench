/*
 * Copyright 2018 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.ndbench.plugin.dynamodb;

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.model.DescribeLimitsResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
import com.netflix.ndbench.plugin.dynamodb.configs.ProgrammaticDynamoDBConfiguration;
import com.netflix.ndbench.plugin.dynamodb.operations.dynamodb.controlplane.CreateDynamoDBTable;
import com.netflix.ndbench.plugin.dynamodb.operations.dynamodb.controlplane.DeleteDynamoDBTable;
import com.netflix.ndbench.plugin.dynamodb.operations.dynamodb.controlplane.DescribeLimits;

/**
 * This NDBench plugin uses DynamoDB's transaction API to write to multiple tables
 * as part of a transaction.
 *
 * @author Sumanth Pasupuleti
 */
@Singleton
@NdBenchClientPlugin("DynamoDBTransactions")
public class DynamoDBTransactions extends DynamoDBKeyValueBase<ProgrammaticDynamoDBConfiguration> implements NdBenchClient {
    private static final Logger logger = LoggerFactory.getLogger(DynamoDBTransactions.class);

    private DynamoDBAutoscalingConfigurer dynamoDBAutoscalingConfigurer;
    private ArrayList<DeleteDynamoDBTable> deleteTables;

    /**
     * Public constructor to inject credentials and configuration
     *
     * @param awsCredentialsProvider
     * @param configuration
     */
    @Inject
    public DynamoDBTransactions(AWSCredentialsProvider awsCredentialsProvider,
                                ProgrammaticDynamoDBConfiguration configuration,
                                DynamoDBAutoscalingConfigurer dynamoDBAutoscalingConfigurer) {
        super(awsCredentialsProvider, configuration);
        this.dynamoDBAutoscalingConfigurer = dynamoDBAutoscalingConfigurer;
    }

    @Override
    public void init(DataGenerator dataGenerator) {
        createAndSetDynamoDBClient();
        instantiateDataPlaneOperations(dataGenerator);

        // prerequisite data from configuration
        String tableName = config.getTableName();
        String partitionKeyName = config.getAttributeName();
        long rcu = Long.parseLong(config.getReadCapacityUnits());
        long wcu = Long.parseLong(config.getWriteCapacityUnits());

        this.deleteTables = new ArrayList<>();
        // create main table object
        CreateDynamoDBTable mainTable = new CreateDynamoDBTable(dynamoDB, tableName, partitionKeyName, rcu, wcu);
        this.deleteTables.add(new DeleteDynamoDBTable(dynamoDB, tableName, partitionKeyName));

        // create child tables objects
        ArrayList<CreateDynamoDBTable> childTables = new ArrayList<>();
        for (int i = 0; i < config.getMainTableColsPerRow(); i++)
        {
            String childTableName = config.getChildTableNamePrefix() + i;
            childTables.add(new CreateDynamoDBTable(dynamoDB, childTableName, partitionKeyName, rcu, wcu));
            deleteTables.add(new DeleteDynamoDBTable(dynamoDB, childTableName, partitionKeyName));
        }

        // execute main table creation
        logger.info("Creating main table programmatically");
        TableDescription td = mainTable.get();
        logger.info("Main Table Description: " + td.toString());

        // execute child tables creation
        logger.info("Creating child table programmatically");
        for (int i = 0; i < config.getMainTableColsPerRow(); i++)
        {
            td = childTables.get(i).get();
            logger.info("Child Table Description: " + td.toString());
        }

        DescribeLimits describeLimits = new DescribeLimits(dynamoDB, tableName, partitionKeyName);
        final DescribeLimitsResult limits = describeLimits.get();
        if (config.getAutoscaling()) {
            // main table
            dynamoDBAutoscalingConfigurer.setupAutoscaling(rcu, wcu, config.getTableName(), limits,
                    Integer.valueOf(config.getTargetWriteUtilization()),
                    Integer.valueOf(config.getTargetReadUtilization()));

            // child tables
            for (int i = 0; i < config.getMainTableColsPerRow(); i++)
            {
                dynamoDBAutoscalingConfigurer.setupAutoscaling(rcu, wcu, config.getChildTableNamePrefix() + i, limits,
                                                               Integer.valueOf(config.getTargetWriteUtilization()),
                                                               Integer.valueOf(config.getTargetReadUtilization()));
            }
        }
    }

    @Override
    public String readSingle(String key)
    {
        // read from main table
        return singleRead.apply(key);
    }

    /**
     * Executes a write transaction, with writes spanning main table and multiple
     * child tables indicated by colsPerRow config property
     * @param key
     * @return
     */
    @Override
    public String writeSingle(String key)
    {
        return transactionWrite.apply(key);
    }

    @Override
    public void shutdown() {
        super.shutdown();
        deleteTables.forEach(deleteTable -> deleteTable.delete());
    }
}
