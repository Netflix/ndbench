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
package com.netflix.ndbench.plugin.dynamodb.operations.controlplane;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.netflix.ndbench.plugin.dynamodb.operations.AbstractDynamoDBOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    public CreateDynamoDBTable(AmazonDynamoDB dynamoDB, String tableName, String partitionKeyName,
                               long readCapacityUnits, long writeCapacityUnits) {
        super(dynamoDB, tableName, partitionKeyName);
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

        logger.debug("Creating table if it does not exist yet");

        // key schema
        ArrayList<KeySchemaElement> keySchema = new ArrayList<>();
        keySchema.add(new KeySchemaElement().withAttributeName(partitionKeyName).withKeyType(KeyType.HASH));

        // Attribute definitions
        ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(new AttributeDefinition().withAttributeName(partitionKeyName)
                .withAttributeType(ScalarAttributeType.S));
        /*
         * constructing the table request: Schema + Attributed definitions + Provisioned
         * throughput
         */
        CreateTableRequest request = new CreateTableRequest().withTableName(tableName)
                .withKeySchema(keySchema).withAttributeDefinitions(attributeDefinitions)
                .withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(readCapacityUnits)
                        .withWriteCapacityUnits(writeCapacityUnits));

        logger.info("Creating Table: " + tableName);

        // Creating table
        if (TableUtils.createTableIfNotExists(dynamoDB, request)) {
            logger.info("Table already exists.  No problem!");
            return dynamoDB.describeTable(tableName).getTable();
        }

        // Waiting util the table is ready
        try {
            logger.debug("Waiting until the table is in ACTIVE state");
            TableUtils.waitUntilActive(dynamoDB, tableName);
            return dynamoDB.describeTable(tableName).getTable();
        } catch (AmazonClientException e) {
            throw new IllegalStateException("Table didn't become active", e);
        } catch (InterruptedException e) {
            throw new IllegalStateException("Table interrupted exception", e);
        }
    }
}
