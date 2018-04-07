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
package com.netflix.ndbench.cli.config;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;
import com.netflix.archaius.api.annotations.PropertyName;
import com.netflix.ndbench.api.plugin.common.NdBenchConstants;

/**
 * This class contains configuration for the CLI.
 * The CLI can pull this configuration from environment variables or from system properties.
 *
 * @author Alexander Patrikalakis
 */
@Configuration(prefix =  NdBenchConstants.PROP_NAMESPACE +  "cli")
public interface CliConfigs {
    @PropertyName(name = "bulkSize")
    @DefaultValue("1")
    String getBulkSize();

    @PropertyName(name = "timeoutMillis")
    @DefaultValue("0")
    String getCliTimeoutMillis();

    @PropertyName(name = "loadPattern")
    @DefaultValue("random")
    String getLoadPattern();

    @PropertyName(name = "windowSize")
    @DefaultValue("-1")
    String getWindowSize();

    @PropertyName(name = "windowDurationInSec")
    @DefaultValue("-1")
    String getWindowDurationInSec();

    @PropertyName(name = "clientName")
    @DefaultValue("InMemoryTest")
    String getClientName();
}
