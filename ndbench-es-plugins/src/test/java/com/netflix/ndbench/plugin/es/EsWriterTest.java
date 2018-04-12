package com.netflix.ndbench.plugin.es;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;


public class EsWriterTest {
    private static final Logger Logger = LoggerFactory.getLogger(EsWriterTest.class);

    @Test
    public void verifyConstructIndexName() throws Exception {
        assert EsWriter.constructIndexName("foo", 0, new Date(0)).equals("foo");

        long exactlyHalfAnHourPassedDawnOfTime = 30 * 60 * 1000;

        int oneHourInMillisecs = 60 * 60 * 1000;
        long exactlyOneHourAndOneMillisecondPassedDawnOfTime = oneHourInMillisecs + 1;

        long oneMillisecondBeforeMidnightOnFirstDayOfTime =   oneHourInMillisecs * 24 - 1;

        /*

        assert EsWriter.constructIndexName("foo", 24, new Date(exactlyHalfAnHourPassedDawnOfTime)).
                equals("foo-1970-01-01.0000");

        assert EsWriter.constructIndexName("foo", 24, new Date(exactlyOneHourAndOneMillisecondPassedDawnOfTime)).
                equals("foo-1970-01-01.0001");
         */

        assert EsWriter.constructIndexName("foo", 48, new Date(exactlyHalfAnHourPassedDawnOfTime)).
                equals("foo-1970-01-01.0001");

        assert EsWriter.constructIndexName("foo", 48, new Date(exactlyHalfAnHourPassedDawnOfTime - 1)).
                equals("foo-1970-01-01.0000");

        assert EsWriter.constructIndexName("foo", 48, new Date(oneMillisecondBeforeMidnightOnFirstDayOfTime)).
                equals("foo-1970-01-01.0047");

        assert EsWriter.constructIndexName("foo", 48, new Date(oneMillisecondBeforeMidnightOnFirstDayOfTime + 2)).
                equals("foo-1970-01-02.0000");
    }


    @Test
    public void verifySuffixesOfRolledIndices() throws Exception {
        int oneMinuteInMillis = 60 * 1000;
        int simulatedNumberOfHours = 10;
        long millisecsSinceEpochStart = 0;
        ArrayList<String> indexNames = new ArrayList<String>();

        int loopTimes = 60 /*minutes per hour */ * simulatedNumberOfHours;
        for (int i = 0; i < loopTimes; i++) {
            String indexName = EsWriter.constructIndexName("foo", 60 * 24, new Date(millisecsSinceEpochStart));
            millisecsSinceEpochStart = millisecsSinceEpochStart + oneMinuteInMillis;
            indexNames.add(indexName);
            Logger.info("indexName:" + indexName);
        }

        assert indexNames.get(59).equals("foo-1970-01-01.0059");
        assert indexNames.get(60).equals("foo-1970-01-01.0060");
        assert indexNames.get(loopTimes - 1).equals("foo-1970-01-01.0599");
    }
}



