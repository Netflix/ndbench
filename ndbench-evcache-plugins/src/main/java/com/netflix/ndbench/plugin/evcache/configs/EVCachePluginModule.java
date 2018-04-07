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
package com.netflix.ndbench.plugin.evcache.configs;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.discovery.guice.EurekaModule;
import com.netflix.evcache.EVCacheModule;
import com.netflix.evcache.connection.ConnectionModule;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPluginGuiceModule;

/**
 * 
 * @author ipapapa
 *
 */
@NdBenchClientPluginGuiceModule
public class EVCachePluginModule extends AbstractModule {
    @Override
    protected void configure() {
        install(new EurekaModule());
        install(new EVCacheModule());
        install(new ConnectionModule());
        
    }

    @Provides
    EVCacheConfigs getEVCachePlugins(ConfigProxyFactory factory) {
        return factory.newProxy(EVCacheConfigs.class);
    }
}
