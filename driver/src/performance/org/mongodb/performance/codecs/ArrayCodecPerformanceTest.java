/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mongodb.performance.codecs;

import org.bson.BSONWriter;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.codecs.ArrayCodec;
import org.mongodb.codecs.Codecs;
import org.mongodb.codecs.PrimitiveCodecs;

import java.lang.reflect.Array;

import static java.lang.String.format;
import static org.mongodb.performance.codecs.PerfTestUtils.NUMBER_OF_NANO_SECONDS_IN_A_SECOND;
import static org.mongodb.performance.codecs.PerfTestUtils.calculateOperationsPerSecond;
import static org.mongodb.performance.codecs.PerfTestUtils.testCleanup;

//CHECKSTYLE:OFF
public class ArrayCodecPerformanceTest {
    private static final int NUMBER_OF_TIMES_FOR_WARMUP = 10000;
    private static final int NUMBER_OF_TIMES_TO_RUN = 100000000;

    private ArrayCodecPerformanceTest.StubCodecs codecs;
    private final MyPrimitiveCodecs primitiveCodecs = new MyPrimitiveCodecs();

    @Before
    public void setUp() throws Exception {
        codecs = new StubCodecs();
    }

    @Test
    public void outputPerformanceForIntArray() throws Exception {
        int[] intArrayToEncode = {1, 2, 3};
        encodeInts(NUMBER_OF_TIMES_FOR_WARMUP, intArrayToEncode);

        for (int i = 0; i < 3; i++) {
            long startTime = System.nanoTime();
            encodeInts(NUMBER_OF_TIMES_TO_RUN, intArrayToEncode);
            long endTime = System.nanoTime();

            outputResults(startTime, endTime);
            testCleanup();
        }
    }

    @Test
    public void outputPerformanceForIntArrayUsingReflection() throws Exception {
        int[] intArrayToEncode = {1, 2, 3};
        encodeUsingReflection(NUMBER_OF_TIMES_FOR_WARMUP, intArrayToEncode);

        for (int i = 0; i < 3; i++) {
            long startTime = System.nanoTime();
            encodeUsingReflection(NUMBER_OF_TIMES_TO_RUN, intArrayToEncode);
            long endTime = System.nanoTime();

            outputResults(startTime, endTime);
            testCleanup();
        }
    }

    @Test
    public void outputPerformanceForStringArray() throws Exception {
        String[] stringArrayToEncode = {"1", "2", "3"};
        encodeStrings(NUMBER_OF_TIMES_FOR_WARMUP, stringArrayToEncode);

        for (int i = 0; i < 3; i++) {
            long startTime = System.nanoTime();
            encodeStrings(NUMBER_OF_TIMES_TO_RUN, stringArrayToEncode);
            long endTime = System.nanoTime();

            outputResults(startTime, endTime);
            testCleanup();
        }
    }

    @Test
    public void outputPerformanceForStringArrayUsingReflection() throws Exception {
        String[] stringArrayToEncode = {"1", "2", "3"};
        encodeUsingReflection(NUMBER_OF_TIMES_FOR_WARMUP, stringArrayToEncode);

        for (int i = 0; i < 3; i++) {
            long startTime = System.nanoTime();
            encodeUsingReflection(NUMBER_OF_TIMES_TO_RUN, stringArrayToEncode);
            long endTime = System.nanoTime();

            outputResults(startTime, endTime);
            testCleanup();
        }
    }

    @Test
    public void outputPerformanceForIntArrayOnRealArrayCodec() throws Exception {
        //80,639,243 ops per second
        int[] intArrayToEncode = {1, 2, 3};
        StubBSONWriter bsonWriter = new StubBSONWriter();
        ArrayCodec codec = new ArrayCodec(null);

        for (int i2 = NUMBER_OF_TIMES_FOR_WARMUP; i2 != 0; i2--) {
            codec.encode(bsonWriter, intArrayToEncode);
        }

        for (int i = 0; i < 3; i++) {
            long startTime = System.nanoTime();
            for (int i1 = NUMBER_OF_TIMES_TO_RUN; i1 != 0; i1--) {
                codec.encode(bsonWriter, intArrayToEncode);
            }
            long endTime = System.nanoTime();

            System.out.println(format("Objects encoded: %d", bsonWriter.getNumberOfStringsEncoded()));
            long timeTakenInNanos = endTime - startTime;
            System.out.println(format("Test took: %,d ns", timeTakenInNanos));
            System.out.println(format("Test took: %,.3f seconds", timeTakenInNanos / NUMBER_OF_NANO_SECONDS_IN_A_SECOND));
            System.out.println(format("%,.0f ops per second%n", calculateOperationsPerSecond(timeTakenInNanos,
                                                                                             NUMBER_OF_TIMES_TO_RUN)));
            testCleanup();
        }
    }

    @Test
    public void outputPerformanceForIntArrayDisguisedAsObjectOnRealArrayCodec() throws Exception {
        //81,505,837 ops per second
        //no real difference when you have to do an instanceof
        //even if the instanceof is the last one in the chain
        Object intArrayToEncode = new int[]{1, 2, 3};
        StubBSONWriter bsonWriter = new StubBSONWriter();
        ArrayCodec codec = new ArrayCodec(null);

        for (int i2 = NUMBER_OF_TIMES_FOR_WARMUP; i2 != 0; i2--) {
            codec.encode(bsonWriter, intArrayToEncode);
        }

        for (int i = 0; i < 3; i++) {
            long startTime = System.nanoTime();
            for (int i1 = NUMBER_OF_TIMES_TO_RUN; i1 != 0; i1--) {
                codec.encode(bsonWriter, intArrayToEncode);
            }
            long endTime = System.nanoTime();

            System.out.println(format("Objects encoded: %d", bsonWriter.getNumberOfStringsEncoded()));
            long timeTakenInNanos = endTime - startTime;
            System.out.println(format("Test took: %,d ns", timeTakenInNanos));
            System.out.println(format("Test took: %,.3f seconds", timeTakenInNanos / NUMBER_OF_NANO_SECONDS_IN_A_SECOND));
            System.out.println(format("%,.0f ops per second%n", calculateOperationsPerSecond(timeTakenInNanos,
                                                                                             NUMBER_OF_TIMES_TO_RUN)));
            testCleanup();
        }
    }

    private void outputResults(final long startTime, final long endTime) {
        System.out.println(format("Objects encoded: %d", codecs.objectsEncoded));
        System.out.println(format("Primitive Objects encoded: %d", primitiveCodecs.objectsEncoded));
        long timeTakenInNanos = endTime - startTime;
        System.out.println(format("Test took: %,d ns", timeTakenInNanos));
        System.out.println(format("Test took: %,.3f seconds", timeTakenInNanos / NUMBER_OF_NANO_SECONDS_IN_A_SECOND));
        System.out.println(format("%,.0f ops per second%n", calculateOperationsPerSecond(timeTakenInNanos,
                                                                                         NUMBER_OF_TIMES_TO_RUN)));
    }
    //CHECKSTYLE:ON

    private void encodeStrings(final int numberOfIterations, final String[] itemToEncode) {
        for (int i = numberOfIterations; i != 0; i--) {
            encodeArrayOfStrings(codecs, itemToEncode);
        }
    }

    private void encodeUsingReflection(final int numberOfIterations, final Object itemToEncode) {
        for (int i = numberOfIterations; i != 0; i--) {
            encodeArrayUsingReflection(codecs, itemToEncode);
        }
    }

    private void encodeInts(final int numberOfIterations, final int[] itemToEncode) {
        for (int i = numberOfIterations; i != 0; i--) {
            encodeArrayOfInts(codecs, itemToEncode);
        }
    }


    private class StubCodecs extends Codecs {
        private int objectsEncoded = 0;

        public StubCodecs() {
            super(null, null);
        }

        @Override
        public void encode(final BSONWriter writer, final Object object) {
            objectsEncoded++;
        }

        @Override
        public void encode(final BSONWriter writer, final Iterable<?> value) {
            objectsEncoded++;
        }
    }

    private final class MyPrimitiveCodecs extends PrimitiveCodecs {
        private int objectsEncoded = 0;

        private MyPrimitiveCodecs() {
            super(null, null, null);
        }

        @Override
        public void encode(final BSONWriter writer, final Object value) {
            objectsEncoded++;
        }
    }


    // These are the two implementations we want to test
    // Firstly, using iteration over an array
    private void encodeArrayOfInts(final Codecs theCodecs, final int[] iterable) {
        //130,685,642 ops per second using iterable
        //two orders of magnitude faster than using reflection
        for (final int anIterable : iterable) {
            theCodecs.encode(null, anIterable);
        }
    }

    private void encodeArrayOfStrings(final Codecs theCodecs, final Object[] iterable) {
        //222,804,981 ops per second iterating over a String array
        //not sure why this would be twice as fast as an int array
        for (final Object value : iterable) {
            theCodecs.encode(null, value);
        }
    }

    // Secondly, using reflection to get the items of the array
    private void encodeArrayUsingReflection(final Codecs theCodecs, final Object value) {
        //2,503,318 ops per second using reflection (for ints)
        //4,051,509 ops per second using reflection (for Strings)
        //two orders of magnitude slower using reflection
        int size = Array.getLength(value);
        for (int i = 0; i < size; i++) {
            theCodecs.encode(null, Array.get(value, i));
        }
    }

}
