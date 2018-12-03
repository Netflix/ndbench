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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.netflix.ndbench.api.plugin.DataGenerator;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author Alexander Patrikalakis
 * @author ipapapa
 */
public class DynamoDBReadBulk extends AbstractDynamoDBReadOperation<BatchGetItemResponse, List<String>, List<String>> {
    public DynamoDBReadBulk(DataGenerator dataGenerator, DynamoDbAsyncClient dynamoDB, String tableName,
                            String partitionKeyName, boolean consistentRead, ReturnConsumedCapacity returnConsumedCapacity) {
        super(dataGenerator, dynamoDB, tableName, partitionKeyName, consistentRead, returnConsumedCapacity,
                "BatchGetItem");
    }

    @Override
    public List<String> apply(List<String> keys) {
        Preconditions.checkArgument(new HashSet<>(keys).size() == keys.size());
        final KeysAndAttributes keysAndAttributes = generateReadRequests(keys);
        readUntilDone(keysAndAttributes);
        return keysAndAttributes.keys().stream()
                .map(Map::toString)
                .collect(Collectors.toList());
    }

    @Override
    public BatchGetItemResponse measureConsumedCapacity(BatchGetItemResponse result) {
        if (result.consumedCapacity() != null) {
            consumed.addAndGet(getConsumedCapacityForTable(result.consumedCapacity()));
        }
        return result;
    }

    private KeysAndAttributes generateReadRequests(List<String> keys) {
        return KeysAndAttributes.builder().keys(keys.stream()
                .map(key -> ImmutableMap.of("id", AttributeValue.builder().s(key).build()))
                .collect(Collectors.toList()))
                .consistentRead(consistentRead).build();
    }

    private void readUntilDone(KeysAndAttributes keysAndAttributes) {
        KeysAndAttributes remainingKeys = keysAndAttributes;
        BatchGetItemResponse response;
        do {
            response = runBatchGetRequest(remainingKeys.toBuilder().consistentRead(consistentRead).build());
            remainingKeys = response.unprocessedKeys().get(tableName);
        } while (remainingKeys != null && remainingKeys.keys() != null && !remainingKeys.keys().isEmpty());
    }

    private BatchGetItemResponse runBatchGetRequest(KeysAndAttributes keysAndAttributes) {
        // TODO self throttle and estimate size of requests
        return Optional.of(BatchGetItemRequest.builder()
                .requestItems(ImmutableMap.of(tableName, keysAndAttributes)).build())
                .map(dynamoDB::batchGetItem)
                .map(super::completeAndCatch)
                .map(this::measureConsumedCapacity)
                .orElse(null);
    }
}
