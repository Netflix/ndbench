package com.netflix.ndbench.plugin.dynamodb.configs;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPluginGuiceModule;

/**
 * 
 * @author ipapapa
 *
 */
@NdBenchClientPluginGuiceModule
public class DynamoDBModule extends AbstractModule {

    @Override
    protected void configure() {
    }

    @Provides
    DynamoDBConfigs getDynamoDBConfigs(ConfigProxyFactory factory) {
        return factory.newProxy(DynamoDBConfigs.class);
    }

}