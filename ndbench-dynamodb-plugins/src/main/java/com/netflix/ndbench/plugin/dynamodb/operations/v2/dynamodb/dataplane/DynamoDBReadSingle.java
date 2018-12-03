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

import com.google.common.collect.ImmutableMap;
import com.netflix.ndbench.api.plugin.DataGenerator;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity;

import java.util.Map;
import java.util.Optional;

/**
 * @author Alexander Patrikalakis
 * @author ipapapa
 */
public class DynamoDBReadSingle extends AbstractDynamoDBReadOperation<GetItemResponse, String, String> {
    public DynamoDBReadSingle(DataGenerator dataGenerator, DynamoDbAsyncClient dynamoDB, String tableName,
                              String partitionKeyName, boolean consistentRead,
                              ReturnConsumedCapacity returnConsumedCapacity) {
        super(dataGenerator, dynamoDB, tableName, partitionKeyName, consistentRead, returnConsumedCapacity, "GetItem");
    }

    @Override
    public String apply(String key) {
        final GetItemRequest request = GetItemRequest.builder()
                .key(ImmutableMap.of(partitionKeyName, AttributeValue.builder().s(key).build()))
                .returnConsumedCapacity(returnConsumedCapacity)
                .consistentRead(consistentRead)
                .tableName(tableName)
                .build();
        return Optional.ofNullable(dynamoDB.getItem(request))
                .map(super::completeAndCatch)
                .map(this::measureConsumedCapacity)
                .map(GetItemResponse::item)
                .map(Map::toString)
                .orElse(null);
    }

    @Override
    public GetItemResponse measureConsumedCapacity(GetItemResponse result) {
        if (result.consumedCapacity() != null) {
            consumed.addAndGet(result.consumedCapacity().capacityUnits());
        }
        return result;
    }
}
