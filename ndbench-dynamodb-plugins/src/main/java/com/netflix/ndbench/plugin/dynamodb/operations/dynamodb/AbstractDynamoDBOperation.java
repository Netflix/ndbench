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
package com.netflix.ndbench.plugin.dynamodb.operations.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.google.common.base.Preconditions;
import com.netflix.ndbench.plugin.dynamodb.operations.AbstractAwsOperation;
import org.apache.commons.lang3.StringUtils;

/**
 * @author Alexander Patrikalakis
 * @author ipapapa
 */
public abstract class AbstractDynamoDBOperation extends AbstractAwsOperation {
    protected static final String ATTRIBUTE_NAME = "value";

    protected final AmazonDynamoDB dynamoDB;
    protected final String tableName;
    protected final String partitionKeyName;

    protected AbstractDynamoDBOperation(AmazonDynamoDB dynamoDB,
                                        String tableName,
                                        String partitionKeyName) {
        Preconditions.checkNotNull(dynamoDB, "DynamoDB client must not be null");
        Preconditions.checkArgument(StringUtils.isNotEmpty(tableName), "Invalid table name");
        Preconditions.checkArgument(StringUtils.isNotEmpty(partitionKeyName), "Invalid partition key name");
        this.dynamoDB = dynamoDB;
        this.tableName = tableName;
        this.partitionKeyName = partitionKeyName;
    }
}
