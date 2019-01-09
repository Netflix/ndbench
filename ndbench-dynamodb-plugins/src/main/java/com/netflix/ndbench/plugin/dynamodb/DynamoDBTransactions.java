/*
 * Copyright 2018 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.ndbench.plugin.dynamodb;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
import com.netflix.ndbench.plugin.dynamodb.configs.DynamoDBConfiguration;

/**
 * This NDBench plugin uses DynamoDB's transaction API to write to multiple tables
 * as part of a transaction.
 *
 * @author Sumanth Pasupuleti
 */
@Singleton
@NdBenchClientPlugin("DynamoDBTransactions")
public class DynamoDBTransactions extends DynamoDBKeyValue {

    /**
     * Public constructor to inject credentials and configuration
     *
     * @param awsCredentialsProvider
     * @param configuration
     */
    @Inject
    public DynamoDBTransactions(AWSCredentialsProvider awsCredentialsProvider, DynamoDBConfiguration configuration) {
        super(awsCredentialsProvider, configuration);
    }

    /**
     * Executes a write transaction, with writes spanning main table and multiple
     * child tables indicated by colsPerRow config property
     * @param key
     * @return
     */
    @Override
    public String writeSingle(String key)
    {
        return transactionWrite.apply(key);
    }
}
