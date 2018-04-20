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
package com.netflix.ndbench.plugin.dynamodb.operations.v2.dynamodb.dataplane;

import com.netflix.ndbench.api.plugin.DataGenerator;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity;

/**
 * @author Alexander Patrikalakis
 */
public abstract class AbstractDynamoDBReadOperation<T, I, O> extends AbstractDynamoDBDataPlaneOperation<T, I, O> {
    protected final boolean consistentRead;

    protected AbstractDynamoDBReadOperation(DataGenerator dataGenerator, DynamoDbAsyncClient dynamoDB, String tableName,
                                            String partitionKeyName, boolean consistentRead,
                                            ReturnConsumedCapacity returnConsumedCapacity, String operationName) {
        super(dynamoDB, tableName, partitionKeyName, dataGenerator, returnConsumedCapacity, operationName);
        this.consistentRead = consistentRead;
    }
}
