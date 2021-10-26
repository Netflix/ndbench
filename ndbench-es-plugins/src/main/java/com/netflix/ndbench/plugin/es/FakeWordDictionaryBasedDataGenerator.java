/*
 *  Copyright 2021 Netflix, Inc.
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

import com.netflix.ndbench.api.plugin.DataGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Delegates all method calls except for getRandomString and getRandomValue to the generator passed to this
 * class's constructor, such that those two methods will return a string value that is actually not random,
 * but is instead drawn from a dictionary of fake words starting with a defined prefix. This dictionary would contain
 * entries such as "nflxab", "nflxyb", "nflxbt", etc.
 * <p>
 * This helps generate documents whose field content is based on a fixed set of (fake) words. And that results in
 * Lucene indices that are closer to those that result from indexing documents that contain natural language sentences.
 * Such indices should be more compact than indices resulting from indexing documents which contain
 * completely random strings.
 */
class FakeWordDictionaryBasedDataGenerator implements DataGenerator {
    private static final Logger logger = LoggerFactory.getLogger(FakeWordDictionaryBasedDataGenerator.class);

    final static int MAX_PAD_CHARS = 10;
    final static int DEFAULT_PAD_CHARS = 4;

    private final char firstValidChar;
    private final char lastValidChar;
    private final DataGenerator dataGeneratorDelegate;
    private final int expectedSizeOfRandomValues;
    private final String[] words;

    /**
     * Constants that help define the domain of generated fake word values.
     */
    private final static String DEFAULT_PREFIX = "nflx";
    private final static char DEFAULT_FIRST_VALID_CHAR = 'a';
    private final static char DEFAULT_LAST_VALID_CHAR = 'j';

    /**
     * A counter to index into the next dictionary word to use when requesting a value.
     */
    private final AtomicInteger wordIndexBaseCounter = new AtomicInteger(0);

    private String[] getWordArray(int numPadChars) {
        if (numPadChars > MAX_PAD_CHARS || numPadChars <= 0) {
            throw new IllegalArgumentException("numPadChars has to be in (0, " + MAX_PAD_CHARS + "], was " + numPadChars);
        }

        int numPossibleChars = lastValidChar - firstValidChar + 1;
        if (numPossibleChars <= 0) {
            throw new IllegalArgumentException("lastValidChar must be greater or equal to firstValidChar");
        }

        int totalGeneratedWords = (int) Math.pow(numPossibleChars, numPadChars);
        String[] wordsArray = new String[totalGeneratedWords];

        List<String> wordsList = generate(DEFAULT_PREFIX, numPadChars, firstValidChar, lastValidChar);
        assert wordsList.size() == totalGeneratedWords;

        logger.info("Created FakeWordDictionaryBasedDataGenerator with " + totalGeneratedWords + " words");
        return wordsList.toArray(wordsArray);
    }

    private List<String> generate(String prefix, int remaining, char rangeStart, char rangeEnd) {
        if (remaining <= 0) {
            return Collections.singletonList(prefix);
        }

        List<String> wordsList = new ArrayList<>();
        for (char c = rangeStart; c <= rangeEnd; c++) {
            wordsList.addAll(generate(prefix + c, remaining - 1, rangeStart, rangeEnd));
        }

        return wordsList;
    }

    String[] getWords() {
        return this.words;
    }

    /**
     * Create a fake word dictionary with 4 pad characters which may be any between 'a' and 'j' (a sample pool of 10).
     * This means the size of the dictionary will be 10^4 words.
     */
    FakeWordDictionaryBasedDataGenerator(DataGenerator dataGenerator, int expectedSizeOfRandomValues) {
        this(dataGenerator, expectedSizeOfRandomValues, DEFAULT_PAD_CHARS, DEFAULT_FIRST_VALID_CHAR, DEFAULT_LAST_VALID_CHAR);
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
        this.words = getWordArray(numPadChars);
    }

    @Override
    public String getRandomString() {
        return this.getRandomValue();
    }

    /**
     * Return a sentence composed of a space separated sequence of fake words from the dictionary cropped so
     * that the length is exactly equal to expectedSizeOfRandomValues.
     * <p>
     * Note: this value will NOT be random.
     */
    @Override
    public String getRandomValue() {
        StringBuilder stringBuilder = new StringBuilder();
        while (stringBuilder.length() < this.expectedSizeOfRandomValues) {
            stringBuilder.append(getNextWord());
            stringBuilder.append(" ");
        }

        return stringBuilder.substring(0, this.expectedSizeOfRandomValues);
    }

    private String getNextWord() {
        return this.words[this.wordIndexBaseCounter.getAndIncrement() % this.words.length];
    }

    @Override
    public Integer getRandomInteger() {
        return this.dataGeneratorDelegate.getRandomInteger();
    }

    @Override
    public Integer getRandomIntegerValue() {
        return this.dataGeneratorDelegate.getRandomIntegerValue();
    }
}
