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
package com.netflix.ndbench.plugin.dynamodb;

import com.google.inject.Inject;
import com.netflix.ndbench.plugin.dynamodb.configs.DynamoDBConfiguration;
import org.apache.commons.lang.StringUtils;
import software.amazon.awssdk.core.auth.AwsCredentials;
import software.amazon.awssdk.core.auth.AwsCredentialsProvider;
import software.amazon.awssdk.core.auth.DefaultCredentialsProvider;
import software.amazon.awssdk.core.auth.StaticCredentialsProvider;

/**
 * This is a wrapper of the static credentials in archaius configuration and a default credentials provider.
 */
public class NdbenchAwsCredentialProvider implements AwsCredentialsProvider {
    private final DynamoDBConfiguration config;
    private volatile AwsCredentialsProvider credentialsProvider;

    @Inject
    public NdbenchAwsCredentialProvider(DynamoDBConfiguration config) {
        this.config = config;
        refresh();
    }

    private void refresh() {
        if (StringUtils.isNotEmpty(config.accessKey()) && StringUtils.isNotEmpty(config.secretKey())) {
            credentialsProvider
                    = StaticCredentialsProvider.create(AwsCredentials.create(config.accessKey(), config.secretKey()));
        } else {
            credentialsProvider = DefaultCredentialsProvider.create();
        }
    }

    @Override
    public AwsCredentials getCredentials() {
        refresh();
        return credentialsProvider.getCredentials();
    }
}
