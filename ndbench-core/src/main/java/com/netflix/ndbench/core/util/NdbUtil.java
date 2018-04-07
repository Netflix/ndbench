/**
 * Copyright (c) 2018 Netflix, Inc.  All rights reserved.
 */
package com.netflix.ndbench.core.util;

/**
 * Util class for NdBench commonly used util methods
 * @author vchella
 */
public class NdbUtil
{
    private static final String[] BINARY_UNITS = { "bytes", "KB", "MB", "GB", "TB", "PB", "EB" };

    /**
     * FileUtils.byteCountToDisplaySize rounds down the size, hence using this for more precision.
     * @param bytes bytes
     * @return human readable bytes
     */
    public static String humanReadableByteCount(final long bytes)
    {
        final int base = 1024;

        // When using the smallest unit no decimal point is needed, because it's the exact number.
        if (bytes < base) {
            return bytes + " " + BINARY_UNITS[0];
        }

        final int exponent = (int) (Math.log(bytes) / Math.log(base));
        final String unit = BINARY_UNITS[exponent];
        return String.format("%.1f %s", bytes / Math.pow(base, exponent), unit);
    }
}
