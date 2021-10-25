/*
 *  Copyright 2021 Netflix, Inc.
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

import org.apache.http.StatusLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;

import javax.inject.Singleton;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@Singleton
public class GenericEsRestClient implements EsRestClient {
    private static final BasicHeader APPLICATION_JSON_HEADER = new BasicHeader("Content-Type", "application/json");
    private static final BasicHeader APPLICATION_XNDJSON_HEADER = new BasicHeader("Content-Type", "application/x-ndjson");

    private Random random;

    private CloseableHttpClient httpClient;
    private ArrayList<URI> hosts;

    @Override
    public void init(List<URI> hosts, EsConfig config) {
        this.random = new Random();
        this.hosts = new ArrayList<>(hosts);

        RequestConfig requestConfig =
                RequestConfig.custom()
                        .setConnectTimeout(config.getConnectTimeoutSeconds() * 1000)
                        .setSocketTimeout(config.getSocketTimeoutSeconds() * 1000)
                        .setConnectionRequestTimeout(config.getConnectionRequestTimeoutSeconds() * 1000)
                        .build();

        HttpClientBuilder httpClientBuilder =
                HttpClients.custom()
                        .setDefaultRequestConfig(requestConfig)
                        .setRetryHandler(new DefaultHttpRequestRetryHandler(0, false));

        customizeHttpClientBuilder(httpClientBuilder, config);

        this.httpClient = httpClientBuilder.build();
    }

    @Override
    public StatusLine writeSingleDocument(String index, String docType, String id, String document) throws IOException {
        if (this.httpClient == null) {
            throw new RuntimeException("GenericEsRestClient must be initialized");
        }

        HttpPut put = new HttpPut(String.format("%s/%s/%s/%s", selectHost().toString(), index, docType, id));
        put.setEntity(new StringEntity(document));
        put.addHeader(APPLICATION_JSON_HEADER);

        try (CloseableHttpResponse response = this.httpClient.execute(put)) {
            return response.getStatusLine();
        }
    }

    @Override
    public StatusLine readSingleDocument(String index, String docType, String id) throws IOException {
        if (this.httpClient == null) {
            throw new RuntimeException("GenericEsRestClient must be initialized");
        }

        HttpGet get = new HttpGet(String.format("%s/%s/%s/%s", selectHost().toString(), index, docType, id));
        try (CloseableHttpResponse response = this.httpClient.execute(get)) {
            return response.getStatusLine();
        }
    }

    @Override
    public StatusLine writeDocumentsBulk(String bulkPayload) throws IOException {
        if (this.httpClient == null) {
            throw new RuntimeException("GenericEsRestClient must be initialized");
        }

        HttpPost post = new HttpPost(String.format("%s/%s", selectHost().toString(), "_bulk"));
        post.setEntity(new StringEntity(bulkPayload));
        post.addHeader(APPLICATION_XNDJSON_HEADER);

        try (CloseableHttpResponse response = this.httpClient.execute(post)) {
            return response.getStatusLine();
        }
    }

    /**
     * Override this method to add custom configuration to the Apache HttpClient before it is built, called during init phase.
     * @param clientBuilder - HTTP client builder with preset RequestConfig and 0-retry DefaultHttpRequestRetryHandler
     * @param config - ES REST plugin configuration
     */
    protected void customizeHttpClientBuilder(HttpClientBuilder clientBuilder, EsConfig config) {
    }

    protected URI selectHost() {
        if (this.hosts.size() == 1) {
            return this.hosts.get(0);
        } else {
            return this.hosts.get(this.random.nextInt(this.hosts.size()));
        }
    }

    @Override
    public void close() throws IOException {
        this.httpClient.close();
    }
}
