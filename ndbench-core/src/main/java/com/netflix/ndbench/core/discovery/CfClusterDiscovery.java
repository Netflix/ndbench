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
package com.netflix.ndbench.core.discovery;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Pulkit Chandra
 */
public class CfClusterDiscovery implements IClusterDiscovery {
    private static final Logger logger = LoggerFactory.getLogger(CfClusterDiscovery.class.getName());

    @Override
    public List<String> getApps() {
        return Arrays.asList(getVmRouteName());
    }

    private String getVmRouteName() {
        String vcap_application = System.getenv("VCAP_APPLICATION");
        ObjectMapper mapper = new ObjectMapper();
        Map<String, List<String>> vcap_map = new HashMap<>();
        try {
            vcap_map = mapper.readValue(vcap_application.getBytes(), HashMap.class);
        } catch (IOException e) {
            logger.error("Exception while reading vcap_application to Map" + e);
        }
        List<String> uris = vcap_map.get("uris");

        return uris.get(0);
    }

    @Override
    public List<String> getEndpoints(String appName, int defaultPort) {
        return Arrays.asList(getVmRouteName());
    }

}
