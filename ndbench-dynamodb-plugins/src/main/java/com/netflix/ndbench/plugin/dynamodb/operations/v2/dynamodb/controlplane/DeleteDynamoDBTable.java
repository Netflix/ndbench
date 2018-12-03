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
package com.netflix.ndbench.plugin.dynamodb.operations.v2.dynamodb.controlplane;

import com.netflix.ndbench.plugin.dynamodb.operations.v2.dynamodb.AbstractDynamoDBOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;

/**
 * @author ipapapa
 * @author Alexander Patrikalakis
 */
public class DeleteDynamoDBTable extends AbstractDynamoDBOperation {
    private static final Logger logger = LoggerFactory.getLogger(DeleteDynamoDBTable.class);

    public DeleteDynamoDBTable(DynamoDbAsyncClient dynamoDB, String tableName, String partitionKeyName) {
        super(dynamoDB, tableName, partitionKeyName, "DeleteTable");
    }

    public void delete() {
        logger.info("Issuing DeleteTable request for " + tableName);
        try {
            completeAndCatch(dynamoDB.deleteTable(DeleteTableRequest.builder().tableName(tableName).build()));
        } catch (ResourceNotFoundException e) {
            logger.warn("Table is already deleted", e);
        }
    }
}
