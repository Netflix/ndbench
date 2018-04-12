package com.netflix.ndbench.plugin.dynamodb.operations;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkServiceException;

public abstract class AbstractAwsOperation {
    private static final Logger logger = LoggerFactory.getLogger(AbstractAwsOperation.class);

    protected SdkServiceException sdkServiceException(SdkServiceException ase) {
        logger.error("Caught an AmazonServiceException, which means your request made it "
                + "to AWS, but was rejected with an error response for some reason.");
        logger.error("Error Message:    " + ase.getMessage());
        logger.error("HTTP Status Code: " + ase.statusCode());
        logger.error("AWS Error Code:   " + ase.errorCode());
        logger.error("Error Type:       " + ase.errorType());
        logger.error("Request ID:       " + ase.requestId());
        return ase;
    }

    protected SdkClientException sdkClientException(SdkClientException ace) {
        logger.error("Caught an AmazonClientException, which means the client encountered "
                + "a serious internal problem while trying to communicate with AWS, "
                + "such as not being able to access the network.");
        logger.error("Error Message: " + ace.getMessage());
        return ace;
    }
}
