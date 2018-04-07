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
package com.netflix.ndbench.defaultimpl;

import com.google.inject.Injector;
import com.google.inject.servlet.GuiceServletContextListener;
import com.google.inject.servlet.ServletModule;
import com.netflix.ndbench.core.config.GuiceInjectorProvider;
import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.spi.container.servlet.ServletContainer;

import java.util.HashMap;
import java.util.Map;

/**
 * @author vchella
 */
public class InjectedWebListener extends GuiceServletContextListener
{
    @Override
    protected Injector getInjector()
    {
        return new GuiceInjectorProvider().getInjector( new JaxServletModule());
    }

    public static class JaxServletModule extends ServletModule
    {
        @Override
        protected void configureServlets()
        {
            Map<String, String> params = new HashMap<String, String>();
            params.put(PackagesResourceConfig.PROPERTY_PACKAGES, "unbound");
            params.put("com.sun.jersey.config.property.packages", "com.netflix.ndbench.core.resources");
            params.put(ServletContainer.PROPERTY_FILTER_CONTEXT_PATH, "/REST");
            serve("/REST/*").with(GuiceContainer.class, params);
        }
    }

}