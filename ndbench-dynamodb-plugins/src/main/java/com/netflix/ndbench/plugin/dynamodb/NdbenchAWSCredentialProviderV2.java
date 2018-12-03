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
import com.google.inject.Singleton;
import com.netflix.ndbench.aws.config.CredentialsConfiguration;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

/**
 * This credentials provider allows refreshing from Archaius configuration if the Archaius configuration contains
 * credentials. Otherwise, this credentials provider delegates to the default AWS credentials provider chain.
 *
 * @author Alexander Patrikalakis
 * @author ipapapa
 */
@Singleton
public class NdbenchAWSCredentialProviderV2 implements AwsCredentialsProvider {
    private final CredentialsConfiguration config;
    private volatile AwsCredentialsProvider credentialsProvider;

    @Inject
    public NdbenchAWSCredentialProviderV2(CredentialsConfiguration config) {
        this.config = config;
        refresh();
    }

    private void refresh() {
        if (StringUtils.isNotEmpty(config.accessKey()) && StringUtils.isNotEmpty(config.secretKey())) {
            credentialsProvider
                    = StaticCredentialsProvider.create(
                            AwsBasicCredentials.create(config.accessKey(), config.secretKey()));
        } else {
            credentialsProvider = DefaultCredentialsProvider.create();
        }
    }

    @Override
    public AwsCredentials resolveCredentials() {
        refresh();
        return credentialsProvider.resolveCredentials();
    }
}