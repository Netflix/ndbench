package com.netflix.ndbench.plugin.es;

import org.junit.Test;

import static org.junit.Assert.*;


public class FakeWordDictionaryBasedDataGeneratorTest {
    @Test
    public void verifyDictionaryHasExpectedWords() {
        String[] array;

        array = new FakeWordDictionaryBasedDataGenerator(null, 0, 1, 'x', 'x').getWords();
        assertNotNull(array);
        assertEquals(1, array.length);
        assertEquals("nflxx", array[0]);

        array = new FakeWordDictionaryBasedDataGenerator(null, 0, 2, 'x', 'x').getWords();
        assertNotNull(array);
        assertEquals(1, array.length);
        assertEquals("nflxxx", array[0]);

        array = new FakeWordDictionaryBasedDataGenerator(null, 0, 2, 'x', 'y').getWords();
        assertNotNull(array);
        assertEquals(4, array.length);
        assertArrayEquals(new String[]{"nflxxx", "nflxxy", "nflxyx", "nflxyy"}, array);
    }

    @Test(expected = IllegalArgumentException.class)
    public void verifyIllegalArgHandling() {
        new FakeWordDictionaryBasedDataGenerator(null, 0, 1, 'x', 'a');
    }

    @Test
    public void testRandomValues() {
        FakeWordDictionaryBasedDataGenerator generator =
                new FakeWordDictionaryBasedDataGenerator(null, 13, 2, 'a', 'c');

        assertEquals("nflxaa nflxab", generator.getRandomValue());
        assertEquals("nflxac nflxba", generator.getRandomValue());
        assertEquals("nflxbb nflxbc", generator.getRandomValue());
        assertEquals("nflxca nflxcb", generator.getRandomValue());
        assertEquals("nflxcc nflxaa", generator.getRandomValue());
        assertEquals("nflxab nflxac", generator.getRandomValue());
    }
}


