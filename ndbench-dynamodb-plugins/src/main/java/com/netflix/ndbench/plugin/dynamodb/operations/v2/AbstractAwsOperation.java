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
package com.netflix.ndbench.plugin.dynamodb.operations.v2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;

public abstract class AbstractAwsOperation {
    private static final Logger logger = LoggerFactory.getLogger(AbstractAwsOperation.class);

    protected AwsServiceException logAwsServiceException(AwsServiceException ase) {
        logger.error("Caught an AmazonServiceException, which means your request made it "
                + "to AWS, but was rejected with an error response for some reason.");
        logger.error("Error Message:    " + ase.getMessage());
        logger.error("HTTP Status Code: " + ase.statusCode());
        logger.error("AWS Error Code:   " + ase.awsErrorDetails().errorCode());
        logger.error("Request ID:       " + ase.requestId());
        return ase;
    }

    protected SdkClientException logSdkClientException(SdkClientException ace) {
        logger.error("Caught an AmazonClientException, which means the client encountered "
                + "a serious internal problem while trying to communicate with AWS, "
                + "such as not being able to access the network.");
        logger.error("Error Message: " + ace.getMessage());
        return ace;
    }
}
