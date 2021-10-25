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

import com.netflix.ndbench.api.plugin.DataGenerator;
import org.apache.http.StatusLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.UUID;


/**
 * Writes instances of JSON documents returned by {@link EsUtils#createDefaultDocumentAsJson}
 * to Elasticsearch either via single REST calls, or via the bulk write API.
 * <p>
 * The responsibilities of this class are to:
 * <p>
 * - Determine if a particular write operation is done via Elasticsearch's bulk API or not (using the value of
 * the isBulkWrite boolean passed to this class's constructor)
 * <p>
 * - Immediately issue a PUT of the document to Elasticsearch if a given write is determined not
 * to be handled by bulk write.
 * <p>
 * - Maintain a thread local buffer of pending documents to be bulk written.
 * <p>
 * - Flush the pending buffer once its size equals {@link EsConfig#getBulkWriteBatchSize()}.
 * <p>
 * Note that if a document is written via bulk write its isBulkWrite attribute will be "true".
 * Note that any exception thrown will be bubbled up to the driver and
 * will result in a failed write being recorded.
 * <p>
 */
class EsWriter {
    private static final Logger logger = LoggerFactory.getLogger(EsWriter.class);

    private final String indexName;
    private final String docType;
    private final int bulkWriteBatchSize;
    private final boolean isBulkWrite;
    private final int indexRollsPerDay;

    private final DataGenerator dataGenerator;

    /**
     * Returns a writer whose {@link EsWriter#writeDocument } method will issue writes to 'indexName' and 'docType'.
     *
     * @param indexName          - index name to which writes will be targeted (with possibly appended date pattern,
     *                           as determined by {@link EsConfig#getIndexRollsPerDay()}
     * @param docType            - document type (of index named esIndexName) to which writes will be targeted.
     * @param isBulkWrite        - whether to perform 1 write or a batch of writes in the context of a writeSingle call.
     * @param indexRollsPerDay   - a value determined by the configuration setting {@link EsConfig#getIndexRollsPerDay()}
     * @param bulkWriteBatchSize - the size the bulk write queue must reach before a bulk write operation
     *                           is performed and the queue is flushed (ignored if isBulkWrite = false,
     *                           but nevertheless cannot be less than or equal to zero.)
     * @param dataGenerator      - data generator used to inject random values into documents written to Elasticsearch.
     */
    EsWriter(String indexName,
             String docType,
             boolean isBulkWrite,
             int indexRollsPerDay,
             int bulkWriteBatchSize,
             DataGenerator dataGenerator) {
        if (bulkWriteBatchSize < 0) {
            throw new IllegalArgumentException("bulkWriteBatchSize cannot be less than to zero");
        }

        if (!isBulkWrite && indexRollsPerDay > 0) {
            throw new IllegalArgumentException(
                    "getIndexRollsPerDay fast property only makes sense to be set when isBulkWrite is set");
        }

        this.docType = docType;
        this.indexName = indexName;
        this.bulkWriteBatchSize = bulkWriteBatchSize;
        this.isBulkWrite = isBulkWrite;
        this.indexRollsPerDay = indexRollsPerDay;
        this.dataGenerator = dataGenerator;
    }

    /**
     * Issues writes to esIndexName' and esDocType given 'restClient'
     * (which determines the host/port of the Elasticsearch cluster to write to)
     */
    WriteResult writeDocument(EsRestClient restClient, String key, Boolean randomizeKeys) throws Exception {
        if (isBulkWrite) {
            writeBulk(restClient, key, randomizeKeys);
        } else {
            writeSingle(restClient, key, randomizeKeys);
        }

        return WriteResult.PROVISIONAL_RESULT_THAT_ASSUMES_ALL_WENT_WELL;
    }

    private void writeSingle(EsRestClient restClient, String key, Boolean randomizeKeys) throws IOException {
        String randomizedKey = key + (randomizeKeys ? UUID.randomUUID().toString() : "");
        String doc = EsUtils.createDefaultDocumentAsJson(dataGenerator, false);

        StatusLine response = restClient.writeSingleDocument(this.indexName, this.docType, randomizedKey, doc);

        logger.debug("Writing document id=[{}] to index [{}], response=[{}]",
                randomizedKey, indexName, response);

        int responseCode = response.getStatusCode();
        if (responseCode != 200 && responseCode != 201) {
            throw new RuntimeException("Write operation failed for " + this.indexName + ". Response: " + response);
        }
    }

    private void writeBulk(EsRestClient restClient, String key, Boolean randomizeKeys) throws IOException {
        StringBuilder stringBuilder = new StringBuilder();
        String indexName = constructIndexName(this.indexName, this.indexRollsPerDay, new Date());

        for (int i = 0; i < bulkWriteBatchSize; i++) {
            String doc = EsUtils.createDefaultDocumentAsJson(dataGenerator, true);
            String randomizedKey = key + (randomizeKeys ? UUID.randomUUID().toString() : "");
            stringBuilder.append(getBulkWriteEntry(randomizedKey, doc, indexName, this.docType));
        }

        String bulkPayload = stringBuilder.toString();

        StatusLine response = restClient.writeDocumentsBulk(bulkPayload);

        if (logger.isTraceEnabled()) {
            logger.trace("Received [{}] after sending bulk write payload of [{}]", response, bulkPayload);
        } else {
            logger.debug("Received [{}] after sending bulk write payload", response);
        }
    }

    private String getBulkWriteEntry(String key, String doc, String indexName, String docType) {
        String bulkWriteEntry = String.format(
                "{\"index\":{\"_index\":\"%s\",\"_type\":\"%s\",\"_id\":\"%s\"}}\n%s\n",
                indexName, docType, key, doc);
        logger.trace("Bulk write entry for one doc: {}", bulkWriteEntry);
        return bulkWriteEntry;
    }

    /**
     * Methods below are package scoped to facilitate unit testing
     */
    static String constructIndexName(String indexName, int indexRollsPerDay, Date date) {
        if (indexRollsPerDay > 0) {
            ZonedDateTime zdt = ZonedDateTime.ofInstant(date.toInstant(), ZoneId.of("UTC"));

            int minutesPerRoll = 1440 / indexRollsPerDay;
            int minutesElapsedSinceStartOfDay = zdt.getHour() * 60 + zdt.getMinute();
            int nthRoll = minutesElapsedSinceStartOfDay / minutesPerRoll;
            String timestampedIndexName = String.format("%s-%s.%04d", indexName, zdt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")), nthRoll);

            logger.debug("constructIndexName from rolls per day = {} gives: {}", indexRollsPerDay, timestampedIndexName);

            return timestampedIndexName;
        } else {
            return indexName;
        }
    }
}
