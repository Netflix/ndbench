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

/**
 * @author vchella
 */
public class TunableConfig {
    int readRateLimit;
    int writeRateLimit;

    public TunableConfig(IConfiguration configuration)
    {
        this.readRateLimit = configuration.getReadRateLimit();
        this.writeRateLimit = configuration.getWriteRateLimit();
    }

    int getReadRateLimit()
    {
        return readRateLimit;
    }
    int getWriteRateLimit()
    {
        return writeRateLimit;
    }

    public TunableConfig setReadRateLimit(int readRateLimit) {
        this.readRateLimit = readRateLimit;
        return this;
    }

    public TunableConfig setWriteRateLimit(int writeRateLimit) {
        this.writeRateLimit = writeRateLimit;
        return this;
    }

}
