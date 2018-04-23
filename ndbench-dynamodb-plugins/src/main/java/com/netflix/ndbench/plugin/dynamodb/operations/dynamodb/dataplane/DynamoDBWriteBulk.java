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

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.netflix.ndbench.api.plugin.DataGenerator;

import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Alexander Patrikalakis
 * @author ipapapa
 */
public class DynamoDBWriteBulk extends AbstractDynamoDBDataPlaneOperation
        implements CapacityConsumingFunction<BatchWriteItemResult, List<String>, List<String>> {
    public DynamoDBWriteBulk(DataGenerator dataGenerator, AmazonDynamoDB dynamoDB, String tableName,
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
                    .map(WriteRequest::getPutRequest)
                    .map(PutRequest::toString)
                    .collect(Collectors.toList());
        } catch (AmazonServiceException ase) {
            throw amazonServiceException(ase);
        } catch (AmazonClientException ace) {
            throw amazonClientException(ace);
        }
    }

    private List<WriteRequest> generateWriteRequests(List<String> keys) {
        return keys.stream()
                .map(key -> ImmutableMap.of(partitionKeyName, new AttributeValue(key),
                        ATTRIBUTE_NAME, new AttributeValue(this.dataGenerator.getRandomValue())))
                .map(item -> new PutRequest().withItem(item))
                .map(put -> new WriteRequest().withPutRequest(put))
                .collect(Collectors.toList());
    }

    private void writeUntilDone(List<WriteRequest> requests) {
        List<WriteRequest> remainingRequests = requests;
        BatchWriteItemResult result;
        do {
            result = runBatchWriteRequest(remainingRequests);
            remainingRequests = result.getUnprocessedItems().get(tableName);
        } while (remainingRequests!= null && remainingRequests.isEmpty());
    }

    private BatchWriteItemResult runBatchWriteRequest(List<WriteRequest> writeRequests) {
        //todo self throttle
        return measureConsumedCapacity(dynamoDB.batchWriteItem(new BatchWriteItemRequest()
                .withRequestItems(ImmutableMap.of(tableName, writeRequests))
                .withReturnConsumedCapacity(returnConsumedCapacity)));
    }

    @Override
    public BatchWriteItemResult measureConsumedCapacity(BatchWriteItemResult result) {
        consumed.addAndGet(result.getConsumedCapacity() == null ? 0 : getConsumedCapacityForTable(result.getConsumedCapacity()));
        return result;
    }
}
