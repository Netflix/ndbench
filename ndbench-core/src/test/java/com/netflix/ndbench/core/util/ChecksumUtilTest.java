package com.netflix.ndbench.core.util;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Sumanth Pasupuleti
 */
public class ChecksumUtilTest
{
    @Test
    public void testChecksumGenerationAndValidationWithAppendFalse()
    {
        String randomString = RandomStringUtils.random(128);
        String encodedString = CheckSumUtil.appendCheckSumAndEncodeBase64(randomString, false);
        Assert.assertTrue(CheckSumUtil.isChecksumValid(encodedString));
    }

    @Test
    public void testChecksumGenerationAndValidationWithAppendTrue()
    {
        String randomString = RandomStringUtils.random(128);
        String encodedString = CheckSumUtil.appendCheckSumAndEncodeBase64(randomString, true);
        Assert.assertTrue(CheckSumUtil.isChecksumValid(encodedString));
    }
}
