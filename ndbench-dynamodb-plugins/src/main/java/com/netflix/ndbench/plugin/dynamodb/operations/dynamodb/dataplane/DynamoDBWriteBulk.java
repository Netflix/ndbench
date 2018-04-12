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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.netflix.ndbench.api.plugin.DataGenerator;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.services.dynamodb.DynamoDBClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.util.HashSet;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author Alexander Patrikalakis
 * @author ipapapa
 */
public class DynamoDBWriteBulk extends AbstractDynamoDBDataPlaneOperation implements Function<List<String>, List<String>> {
    public DynamoDBWriteBulk(DataGenerator dataGenerator, DynamoDBClient dynamoDB, String tableName,
                             String partitionKeyName, ReturnConsumedCapacity returnConsumedCapacity) {
        super(dynamoDB, tableName, partitionKeyName, dataGenerator, returnConsumedCapacity);
    }

    @Override
    public List<String> apply(List<String> keys) {
        Preconditions.checkArgument(new HashSet<>(keys).size() == keys.size());
        final List<WriteRequest> writeRequests = generateWriteRequests(keys);
        try {
            writeUntilDone(writeRequests);
            return writeRequests.stream()
                    .map(WriteRequest::putRequest)
                    .map(PutRequest::toString)
                    .collect(Collectors.toList());
        } catch (SdkServiceException ase) {
            throw sdkServiceException(ase);
        } catch (SdkClientException ace) {
            throw sdkClientException(ace);
        }
    }

    private List<WriteRequest> generateWriteRequests(List<String> keys) {
        return keys.stream()
                .map(key -> ImmutableMap.of(partitionKeyName, AttributeValue.builder().s(key).build(),
                        ATTRIBUTE_NAME, AttributeValue.builder().s(this.dataGenerator.getRandomValue()).build()))
                .map(item -> PutRequest.builder().item(item).build())
                .map(put -> WriteRequest.builder().putRequest(put).build())
                .collect(Collectors.toList());
    }

    private void writeUntilDone(List<WriteRequest> requests) {
        List<WriteRequest> remainingRequests = requests;
        BatchWriteItemResponse result;
        do {
            result = runBatchWriteRequest(remainingRequests);
            remainingRequests = result.unprocessedItems().get(tableName);
        } while (remainingRequests!= null && remainingRequests.isEmpty());
    }

    public BatchWriteItemResponse measureConsumedCapacity(BatchWriteItemResponse result) {
        consumed.addAndGet(getConsumedCapacityForTable(result.consumedCapacity()));
        return result;
    }
    private BatchWriteItemResponse runBatchWriteRequest(List<WriteRequest> writeRequests) {
        //todo self throttle
        return measureConsumedCapacity(dynamoDB.batchWriteItem(BatchWriteItemRequest.builder()
                .requestItems(ImmutableMap.of(tableName, writeRequests)).build()));
    }
}
