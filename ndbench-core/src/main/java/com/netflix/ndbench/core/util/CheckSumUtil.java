/*
 *  Copyright 2019 Netflix, Inc.
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

package com.netflix.ndbench.core.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Sumanth Pasupuleti
 *
 * CheckSumUtil contains methods around generation and validation of CRC32 based checksum
 */
public class CheckSumUtil
{
    private static final Logger logger = LoggerFactory.getLogger(CheckSumUtil.class);

    /**
     * Generates a checksum of the input string or an abridged version of the input string (depending upon append param)
     * and returns a base64 encoded string of the input string (or an abridged version of it) + checksum.
     * Returned string is usually longer than the input string. 33% overhead comes from base64 encoding, and the rest depends
     * on append param.
     *
     * Future enhancement: This method can be further enhanced by generating checksum for every x-byte block of the input string. Validator can then
     * validate checksum at block level and bail out of parsing further blocks when an invalid checksum is encountered.
     * @param inputString string for which checksum has to be generated and appended to
     * @param append If true, checksum is generated for the entire input string and checksum (8 bytes) is appended to the input string
     *               after which it is base64 encoded.
     *               If false, last 8 bytes of the input string are discarded to make it an abridged version of the input string
     *               and checksum (8 bytes) is appended to the input string after which it is base64 encoded.
     *               This is primarily useful to have a control on the length of the returned string relative to the length of the input string.
     * @return Base64 encoded String of (input string + checksum)
     */
    public static String appendCheckSumAndEncodeBase64(String inputString, boolean append)
    {
        if (!append)
        {
            // crc32 generates a checksum of type long (8 bytes), so we truncate the last 8 bytes of the original string
            // and replace it with the checksum instead of just appending the checksum which is the case if append is false.
            inputString = inputString.substring(0, inputString.length() - 8);
        }

        Checksum checksum = new CRC32();
        byte[] inputStringInBytes = inputString.getBytes(StandardCharsets.UTF_8);
        checksum.update(inputStringInBytes, 0, inputStringInBytes.length);
        byte[] checksumInBytes = ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(checksum.getValue()).array();

        // append input string bytes and checksum bytes
        byte[] output = new byte[inputStringInBytes.length + checksumInBytes.length];
        System.arraycopy(inputStringInBytes, 0, output, 0, inputStringInBytes.length);
        System.arraycopy(checksumInBytes, 0, output, inputStringInBytes.length, checksumInBytes.length);

        // return Base64 encoded string of the resulting concatenated bytes.
        return Base64.getEncoder().encodeToString(output);
    }

    /**
     * Assumes input string is Base64 encoded, and assumes checksum is the last 8 bytes.
     * Base64 decodes the input string, extracts original string bytes and checksum bytes, generates checksum from the
     * extracted string bytes, and validates against the extracted checksum bytes.
     * @param encodedInput
     * @return true if the checksum is correct, false otherwise
     */
    public static boolean isChecksumValid(String encodedInput)
    {
        try
        {
            byte[] inputInBytes = Base64.getDecoder().decode(encodedInput);
            // assumes last 8 bytes to be checksum and remaining bytes to be the original input string
            byte[] extractedInputStringInBytes = Arrays.copyOfRange(inputInBytes, 0, inputInBytes.length - 8);
            byte[] extractedChecksumInBytes = Arrays.copyOfRange(inputInBytes, inputInBytes.length - 8, inputInBytes.length);

            Checksum checksum = new CRC32();
            checksum.update(extractedInputStringInBytes, 0, extractedInputStringInBytes.length);
            byte[] generatedChecksumInBytes = ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(checksum.getValue()).array();
            return Arrays.equals(extractedChecksumInBytes, generatedChecksumInBytes);
        }
        catch (Exception ex)
        {
            logger.error("Exception during checksum validation for encoded input string: {}", encodedInput, ex);
            return false;
        }
    }
}
