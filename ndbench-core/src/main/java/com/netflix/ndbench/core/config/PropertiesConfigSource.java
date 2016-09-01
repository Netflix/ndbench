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
package com.netflix.ndbench.core.config;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.netflix.ndbench.core.util.NdBenchConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Loads the 'Ndbench.properties' file as a source.
 */
public class PropertiesConfigSource extends AbstractConfigSource
{
    private static final Logger logger = LoggerFactory.getLogger(PropertiesConfigSource.class.getName());

    private static final String DEFAULT_NDBENCH_PROPERTIES = NdBenchConstants.WEBAPP_NAME+".properties";

    private final Map<String, String> data = Maps.newConcurrentMap();
    private final String ndBenchFile;

    public PropertiesConfigSource()
    {
        this.ndBenchFile = DEFAULT_NDBENCH_PROPERTIES;
    }

    public PropertiesConfigSource(final Properties properties)
    {
        checkNotNull(properties);
        this.ndBenchFile = DEFAULT_NDBENCH_PROPERTIES;
        clone(properties);
    }

    @VisibleForTesting
    PropertiesConfigSource(final String file)
    {
        this.ndBenchFile = checkNotNull(file);
    }

    @Override
    public void initialize()
    {
        super.initialize();
        Properties properties = new Properties();
        URL url = PropertiesConfigSource.class.getClassLoader().getResource(ndBenchFile);
        if (url != null)
        {
            try
            {
                properties.load(url.openStream());
                clone(properties);
            }
            catch (IOException e)
            {
                logger.info("No Ndbench.properties. Ignore!");
            }
        }
        else
        {
            logger.info("No Ndbench.properties. Ignore!");
        }
    }

    @Override
    public String get(final String prop)
    {
        return data.get(prop);
    }

    @Override
    public void set(final String key, final String value)
    {
        Preconditions.checkNotNull(value, "Value can not be null for configurations.");
        data.put(key, value);
    }


    @Override
    public int size()
    {
        return data.size();
    }

    @Override
    public boolean contains(final String prop)
    {
        return data.containsKey(prop);
    }

    /**
     * Clones all the values from the properties.  If the value is null, it will be ignored.
     *
     * @param properties to clone
     */
    private void clone(final Properties properties)
    {
        if (properties.isEmpty()) return;

        synchronized (properties)
        {
            for (final String key : properties.stringPropertyNames())
            {
                final String value = properties.getProperty(key);
                if (!Strings.isNullOrEmpty(value))
                {
                    data.put(key, value);
                }
            }
        }
    }
}