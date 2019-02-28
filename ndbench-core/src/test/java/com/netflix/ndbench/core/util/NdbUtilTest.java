package com.netflix.ndbench.core.util;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Assert;
import org.junit.Test;

import static com.netflix.ndbench.core.util.NdbUtil.humanReadableByteCount;

/**
 * @author vchella
 */
public class NdbUtilTest
{
    static Collection<Object[]> generateData() {
        return Arrays.asList(new Object[][] { { 0L, "0 bytes" },
                                              { 27L, "27 bytes" }, { 999L, "999 bytes" }, {1000L, "1000 bytes" },
                                              {1023L, "1023 bytes"},{1024L, "1.0 KB"},{1728L, "1.7 KB"},{110592L, "108.0 KB"},
                                              {7077888L, "6.8 MB"}, {452984832L, "432.0 MB"}, {28991029248L, "27.0 GB"},
                                              {1855425871872L, "1.7 TB"}, {9223372036854775807L, "8.0 EB"}});
    }

    @Test
    public void testByteCountToDisplaySizeBigInteger() {
        generateData().forEach(objects ->    Assert.assertEquals(objects[1],
                                                                 humanReadableByteCount(((long)objects[0]))));
    }
}