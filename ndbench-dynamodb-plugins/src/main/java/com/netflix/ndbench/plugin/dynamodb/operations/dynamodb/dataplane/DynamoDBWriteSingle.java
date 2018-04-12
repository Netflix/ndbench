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


import com.google.common.collect.ImmutableMap;
import com.netflix.ndbench.api.plugin.DataGenerator;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.services.dynamodb.DynamoDBClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity;

import java.util.Optional;
import java.util.function.Function;

/**
 * @author Alexander Patrikalakis
 * @author ipapapa
 */
public class DynamoDBWriteSingle extends AbstractDynamoDBDataPlaneOperation implements Function<String, String> {
    public DynamoDBWriteSingle(DataGenerator dataGenerator, DynamoDBClient dynamoDB, String tableName,
                               String partitionKeyName, ReturnConsumedCapacity returnConsumedCapacity) {
        super(dynamoDB, tableName, partitionKeyName, dataGenerator, returnConsumedCapacity);
    }

    @Override
    public String apply(String key) {
        try {
            // Write the item to the table
            return Optional.ofNullable(dynamoDB.putItem(PutItemRequest.builder()
                    .item(ImmutableMap.of(partitionKeyName, AttributeValue.builder().s(key).build(),
                                          ATTRIBUTE_NAME, AttributeValue.builder().s(dataGenerator.getRandomValue()).build())).build()))
                    .map(this::measureConsumedCapacity)
                    .map(PutItemResponse::toString)
                    .orElse(null);
        } catch (SdkServiceException ase) {
            throw sdkServiceException(ase);
        } catch (SdkClientException ace) {
            throw sdkClientException(ace);
        }
    }

    private PutItemResponse measureConsumedCapacity(PutItemResponse result) {
        consumed.addAndGet(result.consumedCapacity().capacityUnits());
        return result;
    }
}
