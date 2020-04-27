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
package com.netflix.ndbench.plugin.configs;

import com.netflix.archaius.api.annotations.DefaultValue;
import com.netflix.archaius.api.annotations.PropertyName;

public interface CassandraConfigurationBase {
    @PropertyName(name = "cluster")
    String getCluster();

    @PropertyName(name = "host")
    // Ignore PMD java rule as inapplicable because we're setting an overridable default
    @SuppressWarnings("PMD.AvoidUsingHardCodedIP")
    // Default to running locally against local C* host
    @DefaultValue("127.0.0.1")
    String getHost();

    @PropertyName(name = "host.port")
    @DefaultValue("9042")
    Integer getHostPort();

    @PropertyName(name = "keyspace")
    @DefaultValue("perftest")
    String getKeyspace();

    @DefaultValue("test1")
    String getCfname();

    @PropertyName(name = "cfname2")
    @DefaultValue("test2")
    String getCfname2();

    @PropertyName(name = "connections")
    @DefaultValue("2")
    Integer getConnections();

    @PropertyName(name = "batchSize")
    @DefaultValue("3")
    Integer getBatchSize();

    @PropertyName(name = "username")
    String getUsername();

    @PropertyName(name = "password")
    String getPassword();

    @PropertyName(name = "useTimestamp")
    @DefaultValue("true")
    Boolean getUseTimestamp();

    @PropertyName(name = "useMultiPartition")
    @DefaultValue("false")
    Boolean getUseMultiPartition();

    @DefaultValue("LOCAL_ONE")
    String getReadConsistencyLevel();

    @DefaultValue("LOCAL_ONE")
    String getWriteConsistencyLevel();

    @DefaultValue("true")
    Boolean getCreateSchema();

    @PropertyName(name = "truststorePath")
    String getTruststorePath();

    @PropertyName(name = "truststorePass")
    String getTruststorePass();
}
