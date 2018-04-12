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
package com.netflix.ndbench.plugin.dynamodb.operations.dynamodb.dataplane;

import com.google.common.util.concurrent.AtomicDouble;
import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.plugin.dynamodb.operations.dynamodb.AbstractDynamoDBOperation;

import java.util.List;
import software.amazon.awssdk.services.dynamodb.DynamoDBClient;
import software.amazon.awssdk.services.dynamodb.model.ConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity;

/**
 * @author Alexander Patrikalakis
 */
public class AbstractDynamoDBDataPlaneOperation extends AbstractDynamoDBOperation {
    protected final DataGenerator dataGenerator;
    protected final AtomicDouble consumed = new AtomicDouble(0.0);
    protected final ReturnConsumedCapacity returnConsumedCapacity;

    protected AbstractDynamoDBDataPlaneOperation(DynamoDBClient dynamoDB, String tableName, String partitionKeyName,
                                                 DataGenerator dataGenerator, ReturnConsumedCapacity returnConsumedCapacity) {
        super(dynamoDB, tableName, partitionKeyName);
        this.dataGenerator = dataGenerator;
        this.returnConsumedCapacity = returnConsumedCapacity;
    }

    protected double getConsumedCapacityForTable(List<ConsumedCapacity> consumedCapacities) {
        return consumedCapacities.stream()
                .filter(c -> tableName.equals(c.tableName()))
                .map(ConsumedCapacity::capacityUnits)
                .findFirst()
                .orElse(0.0);
    }

    public double getAndResetConsumed() {
        return consumed.getAndSet(0.0);
    }
}
