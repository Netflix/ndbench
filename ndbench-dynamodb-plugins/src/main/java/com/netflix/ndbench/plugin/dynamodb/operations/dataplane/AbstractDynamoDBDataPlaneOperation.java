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
package com.netflix.ndbench.plugin.dynamodb.operations.dataplane;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.plugin.dynamodb.operations.AbstractDynamoDBOperation;

/**
 * @author Alexander Patrikalakis
 */
public class AbstractDynamoDBDataPlaneOperation extends AbstractDynamoDBOperation {
    protected final DataGenerator dataGenerator;

    protected AbstractDynamoDBDataPlaneOperation(AmazonDynamoDB dynamoDB, String tableName, String partitionKeyName,
                                                 DataGenerator dataGenerator) {
        super(dynamoDB, tableName, partitionKeyName);
        this.dataGenerator = dataGenerator;
    }
}
