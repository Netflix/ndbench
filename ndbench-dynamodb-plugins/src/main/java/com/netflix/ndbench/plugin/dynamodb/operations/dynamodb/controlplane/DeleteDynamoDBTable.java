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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.netflix.ndbench.plugin.dynamodb.operations.dynamodb.AbstractDynamoDBOperation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author ipapapa
 * @author Alexander Patrikalakis
 */
public class DeleteDynamoDBTable extends AbstractDynamoDBOperation {
    private static final Logger logger = LoggerFactory.getLogger(DeleteDynamoDBTable.class);

    private final Table table;

    public DeleteDynamoDBTable(AmazonDynamoDB dynamoDB, String tableName, String partitionKeyName) {
        super(dynamoDB, tableName, partitionKeyName);
        this.table = new Table(dynamoDB, tableName);
    }

    public void delete() {
        try {
            logger.info("Issuing DeleteTable request for " + tableName);
            table.delete();

            logger.info("Waiting for " + tableName + " to be deleted...this may take a while...");

            table.waitForDelete();
        } catch (ResourceNotFoundException e) {
            logger.info("Table was already deleted");
        } catch (Exception e) {
            throw new IllegalStateException("DeleteTable request failed for " + tableName, e);
        }
    }
}
