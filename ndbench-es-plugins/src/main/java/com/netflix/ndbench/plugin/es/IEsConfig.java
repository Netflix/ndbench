package com.netflix.ndbench.plugin.es;


import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;
import com.netflix.ndbench.api.plugin.common.NdBenchConstants;

@Configuration(prefix =  NdBenchConstants.PROP_NAMESPACE +  ".es")
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
     * used in a <a href=""https://www.elastic.co/guide/en/elasticsearch/reference/current/_batch_processing.html">
     * bulk indexing request</a>. If zero is specified, then no rolling will occur, and no date pattern will be used in
     * the name of the index, otherwise the number specified determines how many times the index will 'roll' per day.
     *
     * If you wish to roll your indices every hour just take the number of times you wish to roll per hour and
     * multiply by 24 (number of hours in a day) to get the total number of rolls per day.
     *
     * Given a return value for this method of 'N' (> 0), then N times per day  the currently-being-written-to
     * index will be closed, and subsequent writes will be directed to a new index with a different name formed by:
     * the prefix determined by {@link #getIndexName(),  today's date (GMT-based) followed by "-N" where
     * 'N' is the number of the roll in a given day.
     *
     * Allowable return values for this function must evenly divide 1440, be at least zero, and be no greater than 1440.
     * The maximum value (1440), would indicate indices are rolled every once every day.
     * <p>
     *
     * Example 1:  to roll twice a day specify '2' as the return value. To roll twice per hour specify 2 * 24 == 48
     * as the return value.
     *
     *
     * Example 2:  if you wanted to roll 7 times per hour you would be out of luck as 1440/(7*24) is not an integer.
     *
     */
    @DefaultValue("0")
    Integer getIndexRollsPerDay();
}
