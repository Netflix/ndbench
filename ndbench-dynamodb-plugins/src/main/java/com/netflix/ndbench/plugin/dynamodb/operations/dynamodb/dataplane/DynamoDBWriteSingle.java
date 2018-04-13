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
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.netflix.ndbench.api.plugin.DataGenerator;

import java.util.Optional;
import java.util.function.Function;

/**
 * @author Alexander Patrikalakis
 * @author ipapapa
 */
public class DynamoDBWriteSingle extends AbstractDynamoDBDataPlaneOperation implements Function<String, String> {
    public DynamoDBWriteSingle(DataGenerator dataGenerator, AmazonDynamoDB dynamoDB, String tableName,
                               String partitionKeyName, ReturnConsumedCapacity returnConsumedCapacity) {
        super(dynamoDB, tableName, partitionKeyName, dataGenerator, returnConsumedCapacity);
    }

    @Override
    public String apply(String key) {
        PutItemRequest request = new PutItemRequest()
                .withReturnConsumedCapacity(returnConsumedCapacity)
                .addItemEntry(partitionKeyName, new AttributeValue().withS(key))
                .addItemEntry(ATTRIBUTE_NAME, new AttributeValue().withS(dataGenerator.getRandomValue()));
        try {
            // Write the item to the table
            return Optional.ofNullable(dynamoDB.putItem(request))
                    .map(this::measureConsumedCapacity)
                    .map(PutItemResult::toString)
                    .orElse(null);
        } catch (AmazonServiceException ase) {
            throw amazonServiceException(ase);
        } catch (AmazonClientException ace) {
            throw amazonClientException(ace);
        }
    }

    private PutItemResult measureConsumedCapacity(PutItemResult result) {
        consumed.addAndGet(result.getConsumedCapacity().getCapacityUnits());
        return result;
    }
}
