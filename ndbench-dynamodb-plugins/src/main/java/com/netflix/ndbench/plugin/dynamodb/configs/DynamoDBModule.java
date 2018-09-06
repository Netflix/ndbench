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
package com.netflix.ndbench.plugin.dynamodb.configs;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPluginGuiceModule;
import com.netflix.ndbench.aws.config.CredentialsConfiguration;

/**
 * 
 * @author ipapapa
 * @author Alexander Patrikalakis
 *
 */
@NdBenchClientPluginGuiceModule
public class DynamoDBModule extends AbstractModule {

    @Override
    protected void configure() {
    }

    @Provides
    DynamoDBConfiguration getDynamoDBConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(DynamoDBConfiguration.class);
    }

    @Provides
    ProgrammaticDynamoDBConfiguration getProgrammaticDynamoDBConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(ProgrammaticDynamoDBConfiguration.class);
    }

    @Provides
    CredentialsConfiguration getCredentialsConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(CredentialsConfiguration.class);
    }
}
