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
package com.netflix.ndbench.aws.config;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.commons.lang3.StringUtils;

/**
 * This credentials provider allows refreshing from Archaius configuration if the Archaius configuration contains
 * credentials. Otherwise, this credentials provider delegates to the default AWS credentials provider chain.
 *
 * @author Alexander Patrikalakis
 * @author ipapapa
 */
@Singleton
public class NdbenchAWSCredentialProvider implements AWSCredentialsProvider {

    private final CredentialsConfiguration config;
    private volatile AWSCredentialsProvider credentialsProvider;

    @Inject
    public NdbenchAWSCredentialProvider(CredentialsConfiguration config) {
        this.config = config;
        refresh();
    }

    @Override
    public AWSCredentials getCredentials() {
        return credentialsProvider.getCredentials();
    }

    @Override
    public void refresh() {
        if (StringUtils.isNotEmpty(config.accessKey()) && StringUtils.isNotEmpty(config.secretKey())) {
            credentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(config.accessKey(), config.secretKey()));
        } else {
            credentialsProvider = DefaultAWSCredentialsProviderChain.getInstance();
        }
        credentialsProvider.refresh();
    }
}