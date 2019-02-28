/*
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.ndbench.core.defaultimpl;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchMonitor;
import com.netflix.ndbench.api.plugin.common.NdBenchConstants;
import com.netflix.ndbench.core.config.IConfiguration;
import com.netflix.ndbench.core.config.NdbenchConfigListener;
import com.netflix.ndbench.core.discovery.*;
import com.netflix.ndbench.core.generators.DefaultDataGenerator;
import com.netflix.ndbench.core.monitoring.FakeMonitor;
import com.netflix.ndbench.core.monitoring.NdBenchDefaultMonitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This module defines the default bindings, this can be customized internally if one has specific services for Service discovery, metrics etc.,
 * @author vchella
 */
public class NdBenchGuiceModule extends AbstractModule {
    private static final Logger logger = LoggerFactory.getLogger(NdBenchGuiceModule.class);


    @Override
    protected void configure() {
        bind(NdBenchMonitor.class).to(NdBenchDefaultMonitor.class);
        String discoveryEnv = System.getenv(NdBenchConstants.DISCOVERY_ENV);
        logger.info("DISCOVERY_ENV is set to: " + discoveryEnv);
        if (discoveryEnv != null && discoveryEnv.equals(NdBenchConstants.DISCOVERY_ENV_CF)) {
            bind(IClusterDiscovery.class).to(CfClusterDiscovery.class);
        } else if (discoveryEnv != null && discoveryEnv.equals(NdBenchConstants.DISCOVERY_ENV_AWS)) {
            bind(IClusterDiscovery.class).to(AWSLocalClusterDiscovery.class);
        } else if (discoveryEnv != null && discoveryEnv.equals(NdBenchConstants.DISCOVERY_ENV_AWS_ASG)) {
            bind(IClusterDiscovery.class).to(AwsAsgDiscovery.class);
        } else if (discoveryEnv != null && discoveryEnv.equals(NdBenchConstants.DISCOVERY_ENV_AWS_CONFIG_FILE)) {
            bind(IClusterDiscovery.class).to(ConfigFileDiscovery.class);
        }
          else {
            bind(IClusterDiscovery.class).to(LocalClusterDiscovery.class);
        }
        bind(DataGenerator.class).to(DefaultDataGenerator.class);
        bind(NdbenchConfigListener.class).asEagerSingleton();
    }

    @Provides
    IConfiguration getIConfiguration(ConfigProxyFactory proxyFactory) {
        return proxyFactory.newProxy(IConfiguration.class);
    }
}
