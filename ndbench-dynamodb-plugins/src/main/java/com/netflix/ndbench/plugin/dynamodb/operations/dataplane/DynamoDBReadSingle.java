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
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.google.common.collect.ImmutableMap;
import com.netflix.ndbench.api.plugin.DataGenerator;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * @author Alexander Patrikalakis
 * @author ipapapa
 */
public class DynamoDBReadSingle extends AbstractDynamoDBReadOperation implements Function<String, String> {
    public DynamoDBReadSingle(DataGenerator dataGenerator, AmazonDynamoDB dynamoDB, String tableName,
                              String partitionKeyName, boolean consistentRead) {
        super(dataGenerator, dynamoDB, tableName, partitionKeyName, consistentRead);
    }

    @Override
    public String apply(String key) {
        final GetItemRequest request = new GetItemRequest()
                .withKey(ImmutableMap.of(partitionKeyName, new AttributeValue(key)))
                .withConsistentRead(consistentRead);
        try {
            return Optional.ofNullable(dynamoDB.getItem(request).getItem())
                    .map(Map::toString)
                    .orElse(null);
        } catch (AmazonServiceException ase) {
            throw amazonServiceException(ase);
        } catch (AmazonClientException ace) {
            throw amazonClientException(ace);
        }
    }
}
