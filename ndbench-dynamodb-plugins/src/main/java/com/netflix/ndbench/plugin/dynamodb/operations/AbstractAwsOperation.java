package com.netflix.ndbench.plugin.dynamodb.operations;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractAwsOperation {
    private static final Logger logger = LoggerFactory.getLogger(AbstractAwsOperation.class);

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
