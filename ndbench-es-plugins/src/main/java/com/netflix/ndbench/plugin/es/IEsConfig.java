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


import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;
import com.netflix.ndbench.api.plugin.common.NdBenchConstants;

@Configuration(prefix =  NdBenchConstants.PROP_NAMESPACE +  "es")
public interface IEsConfig {
    @DefaultValue("es_ndbench_test")
    String getCluster();

    @DefaultValue("")
    String getHostName();

    @DefaultValue("false")
    Boolean isHttps();

    @DefaultValue("ndbench_index")
    String getIndexName();

    @DefaultValue("9200")
    Integer getRestClientPort();


    @DefaultValue("0")
    Integer getBulkWriteBatchSize();


  /**
     * If true then the values of string type fields written to Elasticsearch will be random, and if false the generated
     * values for  fields  will be formed using a 'dictionary' of fake words starting with a defined prefix.
     * This dictionary would contain entries such as: dog-ab, dog-yb, dog-bt, etc.
     * <p>
     * Setting this attribute to false results in Lucene indices that are closer to those that result from
     * indexing  documents that contain natural language sentences. Such indices should be more compact than
     * indices resulting from indexing documents which contain completely random gibberish.
     */
    @DefaultValue("true")
    Boolean isRandomizeStrings();

    /**
     * Used to determine the name of the index to write to when constructing the REST payload of documents and metadata
     * used in a <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/_batch_processing.html">
     * bulk indexing request</a>. If zero is specified, then no rolling will occur, and no date pattern will be used in
     * the name of the index, otherwise the number specified determines how many times the index will 'roll' per day.
     * <p>
     * If you wish to roll your indices every hour just take the number of times you wish to roll per hour and
     * multiply by 24 (number of hours in a day) to get the total number of rolls per day.
     * <p>
     * Given a return value for this method of 'N' greater than 0, then N times per day  the currently-being-written-to
     * index will be closed, and subsequent writes will be directed to a new index with a different name formed by:
     * the prefix determined by {@link #getIndexName()},  today's date (GMT-based) followed by "-N" where
     * 'N' is the number of the roll in a given day.
     * <p>
     * Allowable return values for this function must evenly divide 1440, be at least zero, and be no greater than 1440.
     * The maximum value (1440), would indicate indices are rolled every once every day.
     * <p>
     * Example 1:  to roll twice a day specify '2' as the return value. To roll twice per hour specify 2 * 24 == 48
     * as the return value.
     * <p>
     * Example 2:  if you wanted to roll 7 times per hour you would be out of luck as 1440/(7*24) is not an integer.
     *
     */
    @DefaultValue("0")
    Integer getIndexRollsPerDay();


    /**
     * Sets the corresponding parameter in the underlying
     * <a href="https://hc.apache.org/httpcomponents-client-ga/index.html">http client</a> connection that is
     * <a href="https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/_timeouts.html">configured
     * and used</a> by the ElasticSearch Rest client, which itself is used by the ES_REST Ndbench client plugin.
     * <p>
     * Adjustments to this setting will trigger calls to
     * <a href="https://hc.apache.org/httpcomponents-client-ga/httpclient/apidocs/org/apache/http/client/config/RequestConfig.Builder.html#setConnectionRequestTimeout(int)">
     * the Elasticsearch Rest client's setConnectionRequestTimeout method</a>.
     * <p>
     * Note that the underlying connection's documentation describes values given in milliseconds, whereas this API
     * requires you to specify the value in seconds.
     * <p>
     * Also note: it is advisable to set all timeout parameters that  affect the underlying
     * <a href="https://hc.apache.org/httpcomponents-client-ga/index.html">http client</a> connection to the same value.
     */
    @DefaultValue("120")
    Integer getConnectTimeoutSeconds();

    /**
     * Sets the corresponding parameter in the underlying
     * <a href="https://hc.apache.org/httpcomponents-client-ga/index.html">http client</a> connection that is
     * <a href="https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/_timeouts.html">configured
     * and used</a> by the ElasticSearch Rest client, which itself is used by the ES_REST Ndbench client plugin.
     * <p>
     * Adjustments to this setting will trigger calls to
     * <a href="https://hc.apache.org/httpcomponents-client-ga/httpclient/apidocs/org/apache/http/client/config/RequestConfig.Builder.html#setConnectionRequestTimeout(int)">
     * the Elasticsearch Rest client's setConnectionRequestTimeout method</a>.
     * <p>
     * Note that the underlying connection's documentation describes values given in milliseconds, whereas this API
     * requires you to specify the value in seconds.
     * <p>
     * Also note: it is advisable to set all timeout parameters that  affect the underlying
     * <a href="https://hc.apache.org/httpcomponents-client-ga/index.html">http client</a> connection to the same value.
     */
    @DefaultValue("120")
    Integer getConnectionRequestTimeoutSeconds();

    /**
     * Sets the corresponding parameter in the underlying
     * <a href="https://hc.apache.org/httpcomponents-client-ga/index.html">http client</a> connection that is
     * <a href="https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/_timeouts.html">configured
     * and used</a> by the ElasticSearch Rest client, which itself is used by the ES_REST Ndbench client plugin.
     * <p>
     * Adjustments to this setting will trigger calls to
     * <a href="https://hc.apache.org/httpcomponents-client-ga/httpclient/apidocs/org/apache/http/client/config/RequestConfig.Builder.html#setSocketTimeout(int)">
     * the Elasticsearch Rest client's setSocketTimeout method</a>.
     * <p>
     * Note that the underlying connection's documentation describes values given in milliseconds, whereas this API
     * requires you to specify the value in seconds.
     * <p>
     * Also note: it is advisable to set all timeout parameters that  affect the underlying
     * <a href="https://hc.apache.org/httpcomponents-client-ga/index.html">http client</a> connection to the same value.
     */
    @DefaultValue("120")
    Integer getSocketTimeoutSeconds();

    /**
     * Sets the total timeout across all retries for the
     * <a href="https://artifacts.elastic.co/javadoc/org/elasticsearch/client/elasticsearch-rest-client/5.6.4/org/elasticsearch/client/RestClient.html">
     * ElasticSearch Rest client</a> instance  used by the ES_REST Ndbench client plugin.
     * <p>
     * <p>
     * Note that the API for the
     * <a href="https://artifacts.elastic.co/javadoc/org/elasticsearch/client/elasticsearch-rest-client/5.6.4/org/elasticsearch/client/RestClientBuilder.html">
     *     Elasticsearch Rest client builder </a> requires values given in milliseconds, whereas this API requires you
     * to specify the value in seconds.
     * <p>
     * Also note:  generally, it is advisable to set all timeout parameters that  affect the underlying connection to the
     * same value, but  setting this value to some multiple of  the socket time out value is how you configure
     * allowed retry attempts.
     */
    @DefaultValue("120")
    Integer getMaxRetryTimeoutSeconds();
}
