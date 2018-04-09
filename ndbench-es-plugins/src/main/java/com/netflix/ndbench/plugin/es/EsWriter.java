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

import com.netflix.ndbench.api.plugin.DataGenerator;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Date;
import java.util.UUID;


/**
 * <p>
 * Writes instances of JSON documents returned by {@link EsUtils#createDefaultDocumentAsJson} to Elasticsearch either
 * via single  REST calls, or via the bulk write API.
 * <p>
 * The responsibilities of this class are to:
 * <uL>
 * <li>
 * determine if a particular write operation is done via Elasticsearch's bulk API or not (using the value of
 * the isBulkWrite  boolean passed to this class's constructor)
 * </li>
 * <p>
 * <li>
 * immediately issue a PUT of the document to Elasticsearch if a given write is determined not to be handled by
 * bulk write.
 * </li>
 * <p>
 * <li>
 * maintain a thread local buffer of pending documents to be bulk written.
 * </li>
 * <p>
 * <li>
 * flush the pending buffer once its size equals {@link IEsConfig#getBulkWriteBatchSize()}.
 * </li>
 * <p>
 * </uL>
 * </p>
 * Note that if a document is written via bulk write its isBulkWrite attribute will be "true".
 * <br/>
 * Note that any exception thrown will be bubbled up to the driver and will result in a failed write being recorded.
 * <p>
 */
class EsWriter {
    private static final Logger logger = LoggerFactory.getLogger(EsWriter.class);

    private final String esIndexUrl;
    private final int bulkWriteBatchSize;
    private final String indexName;
    private final String esDocType;
    private final boolean isBulkWrite;

    private static final BasicHeader CONTENT_TYPE_HDR_JSON = new BasicHeader("Content-Type", "application/json");

    private final DataGenerator dataGenerator;
    private final int indexRollsPerDay;


    /**
     * Returns a writer whose {@link EsWriter#writeDocument } method will issue writes to 'esIndexName' and 'esDocType'.
     *
     * @param esIndexName        - index name to which writes will be targeted (with possibly appended date pattern,
     *                           as determined by {@link IEsConfig#getIndexRollsPerDay()}
     * @param esDocType          - document type (of index named esIndexName) to which writes will be targeted
     * @param isBulkWrite        - whether to perform 1 write or a batch of writes in the context of a 'writeSingle'
     *                           call
     * @param indexRollsPerDay  - a value determined by the configuration setting {@link IEsConfig#getIndexRollsPerDay()}
     * @param bulkWriteBatchSize - the size the bulk write queue must reach before a bulk write operation is performed
     *                           and the queue is flushed (ignored if isBulkWrite = false, but nevertheless
     *                           cannot be less than or equal to zero.)
     * @param dataGenerator      - data generator used to inject random values into documents written to Elasticsearch.
     */
    EsWriter(String esIndexName,
             String esDocType,
             boolean isBulkWrite,
             int indexRollsPerDay,
             int bulkWriteBatchSize,
             DataGenerator dataGenerator) {
        if (bulkWriteBatchSize < 0) {
            throw new IllegalArgumentException("bulkWriteBatchSize  cannot be less than to zero");
        }
        if (!isBulkWrite && indexRollsPerDay > 0) {
            throw new IllegalArgumentException(
                    "getIndexRollsPerDay fast property only makes sense to be set when isBulkWrite is set ");
        }

        this.esIndexUrl = esIndexName + "/" + esDocType;
        this.esDocType = esDocType;
        this.indexName = esIndexName;
        this.bulkWriteBatchSize = bulkWriteBatchSize;
        this.isBulkWrite = isBulkWrite;
        this.indexRollsPerDay = indexRollsPerDay;
        this.dataGenerator = dataGenerator;
    }


    /**
     * Issues writes to esIndexName' and esDocType given 'restClient' (which determines the host/port of the
     * Elasticsearch cluster to write to.)
     */
    WriteResult writeDocument(RestClient restClient,
                              String key,
                              Boolean randomizeKeys) throws Exception {
        if (isBulkWrite) {
            writeBatchSizeWorthOfDocs(restClient, key, randomizeKeys);
        } else {
            writeSingleDoc(restClient, key, randomizeKeys);
        }

        return WriteResult.PROVISIONAL_RESULT_THAT_ASSUMES_ALL_WENT_WELL;
    }

    private void writeSingleDoc(RestClient restClient,
                                String key,
                                Boolean randomizeKeys) throws IOException {
        String randomizedKey = key + (randomizeKeys ? UUID.randomUUID().toString() : "");
        String url = "/" + esIndexUrl + "/" + randomizedKey;
        String doc = EsUtils.createDefaultDocumentAsJson(dataGenerator, false);
        Response response =
                restClient.performRequest(
                        "PUT",
                        url,
                        Collections.emptyMap(),
                        new StringEntity(doc),
                        CONTENT_TYPE_HDR_JSON);
        logger.debug(
                "write of doc with key={} to index={} resulted in response: {}", randomizedKey, indexName, response);

        int responseCode = response.getStatusLine().getStatusCode();
        if (responseCode != 200 && responseCode != 201) {
            throw new RuntimeException("write operation failed [" + url + "]. response: " + response);
        }
    }

    private void writeBatchSizeWorthOfDocs(RestClient restClient,
                                           String key,
                                           Boolean randomizeKeys) throws IOException {
        StringBuilder stringBuilder = new StringBuilder();
        String indexName = constructIndexName(this.indexName, indexRollsPerDay, new Date());
        for (int i = 0; i < bulkWriteBatchSize; i++) {
            String doc = EsUtils.createDefaultDocumentAsJson(dataGenerator, true);
            String randomizedKey = key + (randomizeKeys ? UUID.randomUUID().toString() : "");
            stringBuilder.append(jsonForAddingDoc(randomizedKey, doc, indexName));
            stringBuilder.append("\n");
        }
        String json = stringBuilder.toString();
        Response response = restClient.performRequest("POST", "/_bulk", Collections.emptyMap(), new StringEntity(json), CONTENT_TYPE_HDR_JSON);
        if (logger.isTraceEnabled()) {
            logger.trace("got response: {} after sending bulk write payload of: {}", response, json);
        } else {
            logger.debug("GOT response: {} after sending bulk write payload", response);
        }
    }


    private String jsonForAddingDoc(String key, String doc, String indexName) {
        String metadata = String.format(
                "{ \"index\" : { \"_index\" : \"%s\", \"_type\" : \"%s\", \"_id\" : \"%s\" } }",
                indexName,
                esDocType,
                key);
        String retval = metadata + "\n" + doc;           // yes. could do in format, but this is clearer
        logger.trace("bulk write payload for one doc: {}", retval);
        return retval;
    }


    /**
     * methods below are package scoped to facilitate unit testing
     */
    static String constructIndexName(String indexName, int indexRollsPerDay, Date date) {
        if (indexRollsPerDay > 0) {
            ZonedDateTime zdt = ZonedDateTime.ofInstant(date.toInstant(), ZoneId.of("UTC"));

            int minutesPerRoll = 1440 / indexRollsPerDay;
            int minutesElapsedSinceStartOfDay = zdt.getHour() * 60 + zdt.getMinute();
            int nthRoll = minutesElapsedSinceStartOfDay  / minutesPerRoll;
            String retval = String.format("%s-%s.%04d", indexName, zdt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")), nthRoll);
            logger.debug("constructIndexName from rolls per day = {} gives: {}", indexRollsPerDay, retval);
            return retval;
        } else {
            return indexName;
        }
    }
}
