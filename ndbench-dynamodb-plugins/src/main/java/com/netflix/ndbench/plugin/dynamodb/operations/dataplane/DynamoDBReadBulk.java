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

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.netflix.ndbench.api.plugin.DataGenerator;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author Alexander Patrikalakis
 * @author ipapapa
 */
public class DynamoDBReadBulk extends AbstractDynamoDBReadOperation implements Function<List<String>, List<String>> {
    public DynamoDBReadBulk(DataGenerator dataGenerator, AmazonDynamoDB dynamoDB, String tableName,
                            String partitionKeyName, boolean consistentRead) {
        super(dataGenerator, dynamoDB, tableName, partitionKeyName, consistentRead);
    }

    @Override
    public List<String> apply(List<String> keys) {
        Preconditions.checkArgument(new HashSet<>(keys).size() == keys.size());
        final KeysAndAttributes keysAndAttributes = generateReadRequests(keys);
        try {
            readUntilDone(keysAndAttributes);
            return keysAndAttributes.getKeys().stream()
                    .map(Map::toString)
                    .collect(Collectors.toList());
        } catch (AmazonServiceException ase) {
            throw amazonServiceException(ase);
        } catch (AmazonClientException ace) {
            throw amazonClientException(ace);
        }
    }

    private KeysAndAttributes generateReadRequests(List<String> keys) {
        return new KeysAndAttributes()
                .withKeys(keys.stream()
                        .map(key -> ImmutableMap.of("id", new AttributeValue(key)))
                        .collect(Collectors.toList()))
                .withConsistentRead(consistentRead);
    }

    private void readUntilDone(KeysAndAttributes keysAndAttributes) {
        KeysAndAttributes remainingKeys = keysAndAttributes;
        BatchGetItemResult result;
        do {
            remainingKeys.withConsistentRead(consistentRead);
            result = runBatchGetRequest(remainingKeys);
            remainingKeys = result.getUnprocessedKeys().get(tableName);
        } while (remainingKeys != null && remainingKeys.getKeys() != null && !remainingKeys.getKeys().isEmpty());
    }

    private BatchGetItemResult runBatchGetRequest(KeysAndAttributes keysAndAttributes) {
        //estimate size of requests
        //todo self throttle
        return dynamoDB.batchGetItem(new BatchGetItemRequest()
                .withRequestItems(ImmutableMap.of(tableName, keysAndAttributes)));
    }
}
