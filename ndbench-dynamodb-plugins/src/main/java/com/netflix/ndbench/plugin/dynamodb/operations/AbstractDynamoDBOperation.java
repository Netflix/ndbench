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
package com.netflix.ndbench.plugin.dynamodb.operations;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.google.common.base.Preconditions;
import com.netflix.ndbench.plugin.dynamodb.DynamoDBKeyValue;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Alexander Patrikalakis
 * @author ipapapa
 */
public abstract class AbstractDynamoDBOperation {
    protected static final String ATTRIBUTE_NAME = "value";
    private static final Logger logger = LoggerFactory.getLogger(DynamoDBKeyValue.class);

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

    protected AmazonServiceException amazonServiceException(AmazonServiceException ase) {
        logger.error("Caught an AmazonServiceException, which means your request made it "
                + "to AWS, but was rejected with an error response for some reason.");
        logger.error("Error Message:    " + ase.getMessage());
        logger.error("HTTP Status Code: " + ase.getStatusCode());
        logger.error("AWS Error Code:   " + ase.getErrorCode());
        logger.error("Error Type:       " + ase.getErrorType());
        logger.error("Request ID:       " + ase.getRequestId());
        return ase;
    }

    protected AmazonClientException amazonClientException(AmazonClientException ace) {
        logger.error("Caught an AmazonClientException, which means the client encountered "
                + "a serious internal problem while trying to communicate with AWS, "
                + "such as not being able to access the network.");
        logger.error("Error Message: " + ace.getMessage());
        return ace;
    }
}
