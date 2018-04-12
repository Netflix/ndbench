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
package com.netflix.ndbench.plugin.dynamodb.operations.dynamodb.controlplane;

import com.google.common.base.Preconditions;
import com.netflix.ndbench.plugin.dynamodb.operations.dynamodb.AbstractDynamoDBOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.services.dynamodb.DynamoDBClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;

import java.util.ArrayList;
import java.util.function.Supplier;

/**
 * @author ipapapa
 * @author Alexander Patrikalakis
 */
public class CreateDynamoDBTable extends AbstractDynamoDBOperation implements Supplier<TableDescription> {
    private static final Logger logger = LoggerFactory.getLogger(CreateDynamoDBTable.class);
    private final long readCapacityUnits;
    private final long writeCapacityUnits;
    public CreateDynamoDBTable(DynamoDBClient dynamoDB, String tableName, String partitionKeyName,
                               long readCapacityUnits, long writeCapacityUnits) {
        super(dynamoDB, tableName, partitionKeyName);
        Preconditions.checkArgument(readCapacityUnits > 0);
        Preconditions.checkArgument(writeCapacityUnits > 0);
        this.readCapacityUnits = readCapacityUnits;
        this.writeCapacityUnits = writeCapacityUnits;
    }

    @Override
    public TableDescription get() {
        /*
         * Create a table with a primary hash key named 'name', which holds a string.
         * Several properties such as provisioned throughput and atribute names are
         * defined in the configuration interface.
         */
        try {
            DescribeTableResponse describeTableResponse = dynamoDB.describeTable(DescribeTableRequest.builder().tableName(tableName).build());
            logger.info("Not creating table because it exists already");
            return describeTableResponse.table();
        } catch(ResourceNotFoundException e) {
            logger.info("Creating Table: " + tableName);
        }

        // key schema
        ArrayList<KeySchemaElement> keySchema = new ArrayList<>();
        keySchema.add(KeySchemaElement.builder().attributeName(partitionKeyName).keyType(KeyType.HASH).build());

        // Attribute definitions
        ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(AttributeDefinition.builder().attributeName(partitionKeyName)
                .attributeType(ScalarAttributeType.S).build());
        /*
         * constructing the table request: Schema + Attributed definitions + Provisioned
         * throughput
         */
        CreateTableRequest request = CreateTableRequest.builder().tableName(tableName)
                .keySchema(keySchema).attributeDefinitions(attributeDefinitions)
                .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(readCapacityUnits)
                        .writeCapacityUnits(writeCapacityUnits).build()).build();

        // Creating table
        try {
            dynamoDB.createTable(request);
            return dynamoDB.describeTable(DescribeTableRequest.builder().tableName(tableName).build()).table();
        } catch(ResourceInUseException e) {
            logger.info("Table already exists.");
            return dynamoDB.describeTable(DescribeTableRequest.builder().tableName(tableName).build()).table();
        } catch (SdkServiceException ase) {
            throw sdkServiceException(ase);
        } catch (SdkClientException ace) {
            throw sdkClientException(ace);
        }
    }
}

