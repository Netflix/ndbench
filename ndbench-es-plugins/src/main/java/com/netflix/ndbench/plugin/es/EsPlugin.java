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
package com.netflix.ndbench.plugin.es;

import com.google.inject.Singleton;
import com.netflix.archaius.api.PropertyFactory;
import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;


/**
 * @author vchella
 */
@Singleton
@NdBenchClientPlugin("ElasticSearchPlugin")
public class EsPlugin implements NdBenchClient {

    private static final Logger logger = LoggerFactory.getLogger(EsPlugin.class);

    private DataGenerator dataGenerator;

    private TransportClient client;

    private static final String ResultOK = "Ok";
    private static final String CacheMiss = null;
    private String EsIndexName ="blog_index_";

    private String ClusterName="Localhost",
            HostName="127.0.0.1";
    private int Port = 9300;


    @Override
    public void init(DataGenerator dataGenerator, PropertyFactory propertyFactory) throws Exception {
        this.dataGenerator = dataGenerator;
        logger.info("Initializing ElasticSearch Plugin...");

        Date dNow = new Date();
        SimpleDateFormat ft = new SimpleDateFormat("yyyyMMddhhmm");
        EsIndexName += ft.format(dNow);
        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", ClusterName).build();
         client = TransportClient.builder().settings(settings).build()
                 .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(HostName), Port))
         ;
    }

    @Override
    public String readSingle(String key) throws Exception
    {
        GetResponse getResponse = client.prepareGet(EsIndexName, "article", key).execute().actionGet();
        if (getResponse != null && getResponse.isExists()) {
            return ResultOK;
        } else {
            return CacheMiss;
        }
    }

    @Override
    public String writeSingle(String key) throws Exception
    {
        client.prepareIndex(EsIndexName, "article", key)
                .setSource(putJsonDocument("ElasticSearch: Java API",
                        dataGenerator.getRandomValue(),
                        new String[]{"elasticsearch","elastic","search", "elastic search"},
                        "user_"+key)).execute().actionGet();

        return ResultOK;
    }

    /**
     * shutdown the client
     */
    @Override
    public void shutdown() throws Exception {
        client.close();
    }

    /**
     * Get connection info
     */
    @Override
    public String getConnectionInfo() throws Exception {
        return String.format("Cluster Name - %s: Index Name - %s", ClusterName, EsIndexName);

    }

    @Override
    public String runWorkFlow() throws Exception
    {
        return null;
    }


    private static Map<String, Object> putJsonDocument(String title,
                                                       String content, String[] tags, String author) {

        Map<String, Object> jsonDocument = new HashMap<String, Object>();

        jsonDocument.put("title", title);
        jsonDocument.put("content", content);
        jsonDocument.put("tags", tags);
        jsonDocument.put("author", author);

        return jsonDocument;
    }
}

