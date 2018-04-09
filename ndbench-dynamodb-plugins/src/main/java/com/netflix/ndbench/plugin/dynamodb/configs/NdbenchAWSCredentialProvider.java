package com.netflix.ndbench.plugin.dynamodb.configs;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.google.inject.Inject;

public class NdbenchAWSCredentialProvider implements AWSCredentialsProvider {

    @Inject
    private DynamoDBConfigs config;


    @Override
    public AWSCredentials getCredentials() {
        if (config.accessKey() != null && config.secretKey() != null) {
            return new AWSCredentials() {
                @Override
                public String getAWSAccessKeyId() {
                    return config.accessKey();
                }

                @Override
                public String getAWSSecretKey() {
                    return config.secretKey();
                }
            };
        }
        else {
            return null;
        }
    }

    @Override
    public void refresh() {
    }
}