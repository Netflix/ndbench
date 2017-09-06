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

import com.google.common.collect.Maps;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.inject.Singleton;
import com.netflix.ndbench.api.plugin.common.NdBenchConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author vchella
 */
@Singleton
public class ConfigFileDiscovery implements IClusterDiscovery {

    private final Map<String, List<String>> clusterMap = Maps.newConcurrentMap();
    private static final Logger logger = LoggerFactory.getLogger(ConfigFileDiscovery.class.getName());

    public ConfigFileDiscovery()
    {
        InputStream in = this.getClass().getClassLoader().getResourceAsStream(NdBenchConstants.CONFIG_CLUSTER_DISCOVERY_NAME);
        String strClusterData = streamToString(in);
        JsonParser parser = new JsonParser();

        JsonElement jsonElement = parser.parse(strClusterData);
        if(jsonElement!=null)
        {
            for (Map.Entry<String, JsonElement> entry: jsonElement.getAsJsonObject().entrySet())
            {
                if(!entry.getKey().isEmpty()) {
                    List<String> lstEndpoints = new LinkedList<>();
                    JsonArray jsonArray = entry.getValue().getAsJsonArray();
                    for (JsonElement ele : jsonArray) {
                        if(!ele.getAsString().isEmpty())
                        {
                            lstEndpoints.add(ele.getAsString());
                        }
                    }
                    clusterMap.put(entry.getKey(), lstEndpoints);
                }
            }
        }

    }
    @Override
    public List<String> getApps() {
        LinkedList<String> returnLst = new LinkedList<>();
        returnLst.addAll(clusterMap.keySet());
        return returnLst;
    }

    @Override
    public List<String> getEndpoints(String appName, int defaultPort) {
        return clusterMap.get(appName);
    }

    private String streamToString(InputStream inputStream) {
        String returnStr = null;
        try {
            ByteArrayOutputStream result = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int length;
            while ((length = inputStream.read(buffer)) != -1) {
                result.write(buffer, 0, length);
            }
            returnStr = result.toString("UTF-8");
        } catch (Exception e) {
            logger.error(String.format("Exception while loading %s file for cluster discovery", NdBenchConstants.CONFIG_CLUSTER_DISCOVERY_NAME), e);
        }
        return returnStr;
    }
}
