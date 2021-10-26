package com.netflix.ndbench.plugin.es;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;

import static org.junit.Assert.*;


public class EsWriterTest {
    private static final Logger logger = LoggerFactory.getLogger(EsWriterTest.class);

    @Test
    public void verifyConstructIndexName() {
        assertEquals("foo", EsWriter.constructIndexName("foo", 0, new Date(0)));

        long exactlyHalfAnHourPassedDawnOfTime = 30 * 60 * 1000;
        long oneMsBeforeMidnightOnFirstDayOfTime = 1000 * 60 * 60 * 24 - 1;

        assertEquals(
                "foo-1970-01-01.0001",
                EsWriter.constructIndexName("foo", 48, new Date(exactlyHalfAnHourPassedDawnOfTime)));

        assertEquals(
                "foo-1970-01-01.0000",
                EsWriter.constructIndexName("foo", 48, new Date(exactlyHalfAnHourPassedDawnOfTime - 1)));

        assertEquals(
                "foo-1970-01-01.0047",
                EsWriter.constructIndexName("foo", 48, new Date(oneMsBeforeMidnightOnFirstDayOfTime)));

        assertEquals(
                "foo-1970-01-02.0000",
                EsWriter.constructIndexName("foo", 48, new Date(oneMsBeforeMidnightOnFirstDayOfTime + 2)));
    }

    @Test
    public void verifySuffixesOfRolledIndices() {
        int simulatedNumberOfHours = 10;
        long msSinceEpochStart = 0;
        ArrayList<String> indexNames = new ArrayList<>();

        int loopTimes = 60 /*minutes per hour */ * simulatedNumberOfHours;
        for (int i = 0; i < loopTimes; i++) {
            String indexName = EsWriter.constructIndexName("foo", 60 * 24, new Date(msSinceEpochStart));
            msSinceEpochStart = msSinceEpochStart + 60 * 1000;
            indexNames.add(indexName);
            logger.info("indexName: " + indexName);
        }

        assertEquals("foo-1970-01-01.0059", indexNames.get(59));
        assertEquals("foo-1970-01-01.0060", indexNames.get(60));
        assertEquals("foo-1970-01-01.0599", indexNames.get(loopTimes - 1));
    }
}

