package com.netflix.ndbench.plugin.es;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


public class FakeWordDictionaryBasedDataGeneratorTest {
    @Test
    public void verifyDictionaryHasExpectedWords() {
        String[] array;

        array = new FakeWordDictionaryBasedDataGenerator(null, 0, 1, 'x', 'x').getWords();
        assert array.length == 1;
        assert array[0].equals("dogx");

        array = new FakeWordDictionaryBasedDataGenerator(null, 0, 2, 'x', 'x').getWords();
        assert array.length == 1;
        assert array[0].equals("dogxx");

        array = new FakeWordDictionaryBasedDataGenerator(null, 0, 2, 'x', 'y').getWords();
        List<String> words = Arrays.asList(array);
        assert words.equals(ImmutableList.of("dogxx", "dogxy", "dogyx", "dogyy"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void verifyIllegalArgHandling() {
        new FakeWordDictionaryBasedDataGenerator(null, 0, 1, 'x', 'a');
    }

    @SuppressWarnings("AssertWithSideEffects")
    @Test
    public void testRandomValues() {
        FakeWordDictionaryBasedDataGenerator generator = new FakeWordDictionaryBasedDataGenerator(null, 11, 2, 'a', 'c');
        generator.wordIndexBaseCounter = new AtomicInteger(0);

        assert generator.getRandomValue().equals("dogab dogac");
        assert generator.getRandomValue().equals("dogba dogbb");
        assert generator.getRandomValue().equals("dogbc dogca");
        assert generator.getRandomValue().equals("dogcb dogcc");
        assert generator.getRandomValue().equals("dogaa dogab");
        assert generator.getRandomValue().equals("dogac dogba");
    }
}


