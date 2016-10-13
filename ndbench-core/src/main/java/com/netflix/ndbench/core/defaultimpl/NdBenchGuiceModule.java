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

import java.util.Set;

import org.reflections.Reflections;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.MapBinder;
import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
import com.netflix.ndbench.core.config.IConfiguration;
import com.netflix.ndbench.core.config.NdBenchConfiguration;
import com.netflix.ndbench.core.discovery.AWSLocalClusterDiscovery;
import com.netflix.ndbench.core.discovery.IClusterDiscovery;
import com.netflix.ndbench.core.generators.StringDataGenerator;
import com.netflix.ndbench.core.monitoring.FakeMonitor;
import com.netflix.ndbench.core.monitoring.NdBenchMonitor;

/**
 * @author vchella
 */
public class NdBenchGuiceModule extends AbstractModule
{
    private static final org.slf4j.Logger Logger = LoggerFactory.getLogger(NdBenchGuiceModule.class);
    private MapBinder<String, NdBenchClient> maps;

    @Override
    protected void configure()
    {
        bind(IConfiguration.class).to(NdBenchConfiguration.class);
        bind(NdBenchMonitor.class).to(FakeMonitor.class);
        bind(IClusterDiscovery.class).to(AWSLocalClusterDiscovery.class);
        bind(DataGenerator.class).to(StringDataGenerator.class);

        //Get all implementations of NdBenchClient Interface and install them as Plugins
        Reflections reflections = new Reflections("com.netflix.ndbench");
        final Set<Class<?>> classes = reflections.getTypesAnnotatedWith(NdBenchClientPlugin.class);
        for (Class<?> ndb: classes) {
            installNdBenchClientPlugin(ndb);
        }
    }

//    @Provides
//    IConfiguration getIConfiguration(ConfigProxyFactory proxyFactory) {
//       return proxyFactory.newProxy(FakeConfiguration.class);
//    }

    @SuppressWarnings("unchecked")
	private <T> void installNdBenchClientPlugin(Class<?> ndBenchClientImple) {
        if (maps == null) {
            maps = MapBinder.newMapBinder(binder(), String.class, NdBenchClient.class);
        }

        String name = getAnnotationValue(ndBenchClientImple);

        maps.addBinding(name).to((Class<? extends NdBenchClient>) ndBenchClientImple);
    }

    private String getAnnotationValue(Class<?> ndBenchClientImple) {
        String name=ndBenchClientImple.getName();
        try {
            NdBenchClientPlugin annot = ndBenchClientImple.getAnnotation(NdBenchClientPlugin.class);
            name = annot.value();
            Logger.info("Installing NdBenchClientPlugin: "+ndBenchClientImple.getName()+" with Annotation: "+name);
        }
        catch (Exception e)
        {
            Logger.warn("No Annotation found for class :"+ name +", so loading default class name");
        }
        return name;
    }
}
