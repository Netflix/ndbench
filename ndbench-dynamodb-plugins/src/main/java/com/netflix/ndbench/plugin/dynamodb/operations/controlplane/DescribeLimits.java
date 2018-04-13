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
package com.netflix.ndbench.plugin.dynamodb.operations.controlplane;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.DescribeLimitsRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeLimitsResult;
import com.netflix.ndbench.plugin.dynamodb.operations.dynamodb.AbstractDynamoDBOperation;

import java.util.function.Supplier;

/**
 * @author Alexander Patrikalakis
 */
public class DescribeLimits extends AbstractDynamoDBOperation implements Supplier<DescribeLimitsResult> {
    public DescribeLimits(AmazonDynamoDB dynamoDB, String tableName, String partitionKeyName) {
        super(dynamoDB, tableName, partitionKeyName);
    }

    @Override
    public DescribeLimitsResult get() {
        return dynamoDB.describeLimits(new DescribeLimitsRequest());
    }
}
