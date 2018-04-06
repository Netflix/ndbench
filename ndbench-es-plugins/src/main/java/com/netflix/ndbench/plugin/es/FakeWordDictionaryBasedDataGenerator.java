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

import com.google.common.collect.ImmutableList;
import com.netflix.ndbench.api.plugin.DataGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Delegates all method calls except for 'getRandomString()' and   'getRandomValue()'  to  the generator passed to this
 * class's constructor, such that those aforementioned two methods will return a string value that is actually not random,
 * but is instead drawn from a 'dictionary' of fake words starting with a defined prefix. This dictionary would contain
 * entries such as: dog-ab, dog-yb, dog-bt, etc.
 * <p>
 * This helps generate documents whose field content is based on a fixed set of (fake) words. And that results in
 * Lucene indices that are closer to those that result from indexing  documents that contain natural language sentences.
 * Such indices should be more compact than indices resulting from indexing documents which contain
 * completely random gibberish.
 */
class FakeWordDictionaryBasedDataGenerator implements DataGenerator {
    final static int MAX_PAD_CHARS = 10;
    final static int DEFAULT_PAD_CHARS = 4;


    private final char firstValidChar;
    private final char lastValidChar;
    private final DataGenerator dataGeneratorDelegate;
    private final int expectedSizeOfRandomValues;
    private final String[] words;


    /**
     * variables that help define the domain of generated fake word values.
     */
    private final static String PREFIX = "dog";
    private static final char defaultFirstValidCharForSuffixes = 'a';
    private static final char defaultLastValidCharForSuffixes = 'j';

    private static final Logger logger = LoggerFactory.getLogger(EsRestPlugin.class);

    /**
     * A counter to index into the next dictionary word to use when requesting a value.. which turns out to not be
     * so random with this version of DataGenerator.
     * <p>
     * Not private so we can set it to a known value during unit tests.
     */

    AtomicInteger wordIndexBaseCounter = new AtomicInteger(0);

    private String[] getWordArray(int numPadChars) {
        if (numPadChars > MAX_PAD_CHARS || numPadChars <= 0) {
            throw new IllegalArgumentException("numPadChars can't <= zero or > " +
                    MAX_PAD_CHARS + " (was " + numPadChars + ")");
        }

        int numPossibleChars = lastValidChar - firstValidChar + 1;
        if (numPossibleChars <= 0) {
            throw new IllegalArgumentException("lastValidChar must be >=  firstValidChar");
        }

        Double howMany = (Math.pow(numPossibleChars, numPadChars));
        String[] returnArray = new String[howMany.intValue()];

        List<String> result = generate(PREFIX, numPadChars, firstValidChar, lastValidChar);
        assert result.size() == howMany.intValue();


        logger.info("created FakeWordDictionaryBasedDataGenerator with " + howMany + " words");
        return result.toArray(returnArray);
    }

    private List<String> generate(String prefix, int remaining, char rangeStart, char rangeEnd) {
        if (remaining <= 0) {
            return ImmutableList.of(prefix);
        }

        List<String> retval = new ArrayList<String>();
        for (char ch = rangeStart; ch <= rangeEnd; ch++) {
            retval.addAll(generate(prefix + ch, remaining - 1, rangeStart, rangeEnd));
        }
        return retval;
    }


    String[] getWords() {
        return words;
    }


    /**
     * Create a fake word dictionary with 4 pad characters which may be any between 'a' and 'j' (a sample pool of 10).
     * This means the size of the dictionary will be 10^4 (10K) words, slightly below the number of words in a  typical
     * native English speaking adult's vocabulary.
     */
    FakeWordDictionaryBasedDataGenerator(DataGenerator dataGenerator, int expectedSizeOfRandomValues) {
        this(dataGenerator,
                expectedSizeOfRandomValues,
                DEFAULT_PAD_CHARS,
                defaultFirstValidCharForSuffixes,
                defaultLastValidCharForSuffixes);
    }


    FakeWordDictionaryBasedDataGenerator(DataGenerator dataGenerator,
                                         int expectedSizeOfRandomValues,
                                         int numPadChars,
                                         char firstValidChar,
                                         char lastValidChar) {
        this.dataGeneratorDelegate = dataGenerator;
        this.firstValidChar = firstValidChar;

        this.lastValidChar = lastValidChar;
        this.expectedSizeOfRandomValues = expectedSizeOfRandomValues;
        words = getWordArray(numPadChars);
    }


    @Override
    public String getRandomString() {
        return this.getRandomValue();
    }

    /**
     * Return a sentence composed of a space separated sequence of fake words from the dictionary chopped so
     * that the length is exactly equal to 'expectedSizeOfRandomValues'.
     * <p>
     * Despite the name, this value will NOT be random.
     */
    @Override
    public String getRandomValue() {
        StringBuilder buf = new StringBuilder();
        while (buf.length() < expectedSizeOfRandomValues) {
            int index = getIndexIntoWordArray();
            buf.append(words[index]);
            buf.append(" ");
        }

        return buf.toString().substring(0, expectedSizeOfRandomValues);
    }

    private int getIndexIntoWordArray() {
        return Math.abs(wordIndexBaseCounter.incrementAndGet()) % words.length;
    }

    @Override
    public Integer getRandomInteger() {
        return dataGeneratorDelegate.getRandomInteger();
    }

    @Override
    public Integer getRandomIntegerValue() {
        return dataGeneratorDelegate.getRandomIntegerValue();
    }
}
