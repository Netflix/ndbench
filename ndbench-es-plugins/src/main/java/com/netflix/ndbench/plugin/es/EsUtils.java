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

import com.google.gson.Gson;
import com.netflix.ndbench.api.plugin.DataGenerator;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;


public class EsUtils {
    private static final Logger logger = LoggerFactory.getLogger(EsUtils.class);

    private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
    private static final MediaType XML = MediaType.parse("application/xml; charset=utf-8");

    private static final OkHttpClient httpClient = new OkHttpClient();

    public static String httpGet(String url) throws IOException {
        Request request = new Request.Builder().url(url).get().build();
        Response response = httpClient.newCall(request).execute();

        if (response.code() != 200 && response.code() != 201) {
            String message = "Unable to read data from " + url + " response code was: " + response.code();
            logger.error(message);
            throw new IOException(message);
        }

        return response.body().string();
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
        Gson gsonObj = new Gson();         // TODO - consider making one static instance since this class is thread safe
        return gsonObj.toJson(doc);
    }
}
