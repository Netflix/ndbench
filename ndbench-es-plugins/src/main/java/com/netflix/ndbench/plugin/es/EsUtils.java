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

import com.google.gson.Gson;
import com.netflix.ndbench.api.plugin.DataGenerator;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;


public class EsUtils {
    private static final Logger logger = LoggerFactory.getLogger(EsUtils.class);
    private static final Gson gsonObj = new Gson();

    public static String httpGetWithRetries(String url, long maxRetries, long delayBetweenRetriesMs) throws IOException {
        HttpGet get = new HttpGet(url);

        try (CloseableHttpClient httpClient =
                     HttpClients.custom().setRetryHandler((exception, executionCount, context) -> {
                         if (executionCount > maxRetries) {
                             return false;
                         } else {
                             try {
                                 if (delayBetweenRetriesMs > 0) {
                                     Thread.sleep(delayBetweenRetriesMs);
                                 }
                             } catch (InterruptedException e) {
                                 logger.warn("Exception waiting for the next retry", e);
                             }
                             return true;
                         }
                     }).build();
             CloseableHttpResponse response = httpClient.execute(get)) {
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != 200 && statusCode != 201) {
                String message = String.format("Unable to read data from %s, response code: %d", url, statusCode);
                logger.error(message);
                throw new IOException(message);
            }

            return EntityUtils.toString(response.getEntity());
        }
    }

    public static Map<String, Object> createDefaultDocument(DataGenerator dataGenerator) {
        Map<String, Object> defaultDocument = new HashMap<>();

        defaultDocument.put("keyword1", dataGenerator.getRandomValue());
        defaultDocument.put("keyword2", dataGenerator.getRandomValue());
        defaultDocument.put("keyword3", dataGenerator.getRandomValue());
        defaultDocument.put("keywords", dataGenerator.getRandomValue());
        defaultDocument.put("text1", dataGenerator.getRandomValue());
        defaultDocument.put("text2", dataGenerator.getRandomValue());
        defaultDocument.put("long1", dataGenerator.getRandomIntegerValue());
        defaultDocument.put("long2", dataGenerator.getRandomIntegerValue());
        defaultDocument.put("long3", dataGenerator.getRandomIntegerValue());
        defaultDocument.put("long4", dataGenerator.getRandomIntegerValue());
        defaultDocument.put("date1", new Date());
        defaultDocument.put("date2", System.currentTimeMillis());

        Map<String, Object> defaultDocumentObject0 = new HashMap<>();
        defaultDocumentObject0.put("keyword0", dataGenerator.getRandomValue());
        defaultDocumentObject0.put("text0", dataGenerator.getRandomValue());
        defaultDocumentObject0.put("long0", dataGenerator.getRandomIntegerValue());
        defaultDocument.put("object0", defaultDocumentObject0);

        Map<String, Object> defaultDocumentNested0 = new HashMap<>();
        defaultDocumentNested0.put("keyword0", dataGenerator.getRandomValue());
        defaultDocumentNested0.put("text0", dataGenerator.getRandomValue());
        defaultDocumentNested0.put("long0", dataGenerator.getRandomIntegerValue());
        defaultDocument.put("nested0", defaultDocumentNested0);

        return defaultDocument;
    }

    public static String createDefaultDocumentAsJson(DataGenerator dataGenerator, Boolean isBulkWrite) {
        Map<String, Object> doc = createDefaultDocument(dataGenerator);

        if (isBulkWrite) {
            doc.put("isBulkWrite", "true");
        }

        return gsonObj.toJson(doc);
    }
}
