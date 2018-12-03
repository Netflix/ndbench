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
package com.netflix.ndbench.plugin.dynamodb.operations.v2.dynamodb;

import com.google.common.base.Preconditions;
import com.netflix.ndbench.plugin.dynamodb.operations.v2.AbstractAwsOperation;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * @author Alexander Patrikalakis
 * @author ipapapa
 */
public abstract class AbstractDynamoDBOperation extends AbstractAwsOperation {
    protected static final String ATTRIBUTE_NAME = "value";

    protected final DynamoDbAsyncClient dynamoDB;
    protected final String tableName;
    protected final String partitionKeyName;
    private final String operationName;

    protected AbstractDynamoDBOperation(DynamoDbAsyncClient dynamoDB,
                                        String tableName,
                                        String partitionKeyName, String operationName) {
        Preconditions.checkNotNull(dynamoDB, "DynamoDB client must not be null");
        Preconditions.checkArgument(StringUtils.isNotEmpty(tableName), "Invalid table name");
        Preconditions.checkArgument(StringUtils.isNotEmpty(partitionKeyName), "Invalid partition key name");
        this.dynamoDB = dynamoDB;
        this.tableName = tableName;
        this.partitionKeyName = partitionKeyName;
        this.operationName = operationName;
    }

    public <R> R completeAndCatch(CompletableFuture<R> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            throw new RuntimeException(operationName + " request was interrupted.", e);
        } catch (ExecutionException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof AwsServiceException) {
                throw logAwsServiceException((AwsServiceException) cause);
            } else if (cause instanceof SdkClientException) {
                throw logSdkClientException((SdkClientException) cause);
            } else {
                throw new RuntimeException(operationName + " request failed.", cause);
            }
        }
    }
}
