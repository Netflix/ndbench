package com.netflix.ndbench.plugin.es;


import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

@Configuration(prefix = "ndbench.config.es")
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


    /**
     *
     * Threshold write failure ratio beyond which no auto-tune increase will occur. By default if failure rate is
     * grows larger than 1% auto tune triggered rate increases will cease.
     */
    @DefaultValue("0.01F")
        // default threshold set so
    Float getAutoTuneWriteFailureRatioThreshold();
}
