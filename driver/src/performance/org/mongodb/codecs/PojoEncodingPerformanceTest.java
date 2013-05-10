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

package org.mongodb.codecs;

import org.junit.Before;
import org.junit.Test;
import org.mongodb.codecs.pojo.ArrayWrapper;
import org.mongodb.codecs.pojo.EmptyPojo;
import org.mongodb.codecs.pojo.IntWrapper;
import org.mongodb.codecs.pojo.ListWrapper;
import org.mongodb.codecs.pojo.MapWrapper;
import org.mongodb.codecs.pojo.TwoPojoWrapper;
import org.mongodb.codecs.pojo.PojoWrapper;
import org.mongodb.codecs.pojo.StringWrapper;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static org.mongodb.codecs.PerfTestUtils.NUMBER_OF_NANO_SECONDS_IN_A_SECOND;
import static org.mongodb.codecs.PerfTestUtils.calculateOperationsPerSecond;
import static org.mongodb.codecs.PerfTestUtils.testCleanup;

public class PojoEncodingPerformanceTest {
    private static final int NUMBER_OF_TIMES_FOR_WARMUP = 10000;
    private static final int NUMBER_OF_TIMES_TO_RUN = 100000000;
    private StubBSONWriter bsonWriter;

    @Before
    public void setUp() {
        bsonWriter = new StubBSONWriter();
    }

    @Test
    public void outputBaselinePerformanceForPojoWithNoFields() throws Exception {
        final PojoEncoder<EmptyPojo> pojoCodec = new PojoEncoder<EmptyPojo>(Codecs.createDefault());

        //  1,858,098 ops per second - 2 orders of magnitude slower than an empty Document
        // 93,130,421 ops per second when you add the Fields cache
        // 27,140,570 ops per second with ClassModel?
        final EmptyPojo pojo = new EmptyPojo();
        encodePojo(NUMBER_OF_TIMES_FOR_WARMUP, pojoCodec, pojo);

        for (int i = 0; i < 3; i++) {
            final long startTime = System.nanoTime();
            encodePojo(NUMBER_OF_TIMES_TO_RUN, pojoCodec, pojo);
            final long endTime = System.nanoTime();

            outputResults(startTime, endTime);
            testCleanup();
        }
    }

    @Test
    public void outputPerformanceForAPojoWithASingleIntField() throws Exception {
        //33,437,167 for a document
        final PojoEncoder<IntWrapper> pojoCodec = new PojoEncoder<IntWrapper>(Codecs.createDefault());
        //  1,099,716 ops per second
        //  1,129,327 ops per second after caching Fields.  This must not be the cost here
        //  2,932,047 ops per second when you use a Pattern to validate the field name
        // 15,884,160 ops per second for ClassModel
        // 18,218,719 ops per second when using a static value (i.e. not using reflection to get field value) so getting is not expensive
        final IntWrapper pojo = new IntWrapper(34);
        encodePojo(NUMBER_OF_TIMES_FOR_WARMUP, pojoCodec, pojo);

        for (int i = 0; i < 3; i++) {
            final long startTime = System.nanoTime();
            encodePojo(NUMBER_OF_TIMES_TO_RUN, pojoCodec, pojo);
            final long endTime = System.nanoTime();

            outputResults(startTime, endTime);
            testCleanup();
        }
    }

    @Test
    public void outputPerformanceForAPojoWithASingleStringField() throws Exception {
        final PojoEncoder<StringWrapper> pojoCodec = new PojoEncoder<StringWrapper>(Codecs.createDefault());
        //  1,063,513 ops per second
        //  1,043,330 ops per second with caching
        // 13,932,946 ops per second using simple ClassModel
        final StringWrapper pojo = new StringWrapper("theValue");
        encodePojo(NUMBER_OF_TIMES_FOR_WARMUP, pojoCodec, pojo);

        for (int i = 0; i < 3; i++) {
            final long startTime = System.nanoTime();
            encodePojo(NUMBER_OF_TIMES_TO_RUN, pojoCodec, pojo);
            final long endTime = System.nanoTime();

            outputResults(startTime, endTime);
            testCleanup();
        }
    }

    @Test
    public void outputPerformanceForAPojoContainingAnotherPojo() throws Exception {
        final PojoEncoder<PojoWrapper> pojoCodec = new PojoEncoder<PojoWrapper>(Codecs.createDefault());
        //   492,846 ops per second, approx half the speed of a Pojo with a primitive
        //   526,708 ops per second with caching
        // 5,880,696 ops per second with simple ClassModel
        final PojoWrapper pojo = new PojoWrapper(new StringWrapper("theValue"));
        encodePojo(NUMBER_OF_TIMES_FOR_WARMUP, pojoCodec, pojo);

        for (int i = 0; i < 3; i++) {
            final long startTime = System.nanoTime();
            encodePojo(NUMBER_OF_TIMES_TO_RUN, pojoCodec, pojo);
            final long endTime = System.nanoTime();

            outputResults(startTime, endTime);
            testCleanup();
        }
    }

    @Test
    public void outputPerformanceForAPojoContainingTwoPojos() throws Exception {
        final PojoEncoder<TwoPojoWrapper> pojoCodec = new PojoEncoder<TwoPojoWrapper>(Codecs.createDefault());
        //   275,176 ops per second
        //   292,063 ops per second with cache
        // 3,251,243 ops per second with simple ClassModel
        final TwoPojoWrapper pojo = new TwoPojoWrapper(new StringWrapper("theValue"), new IntWrapper(8373));
        encodePojo(NUMBER_OF_TIMES_FOR_WARMUP, pojoCodec, pojo);

        for (int i = 0; i < 3; i++) {
            final long startTime = System.nanoTime();
            encodePojo(NUMBER_OF_TIMES_TO_RUN, pojoCodec, pojo);
            final long endTime = System.nanoTime();

            outputResults(startTime, endTime);
            testCleanup();
        }
    }

    @Test
    public void outputPerformanceForIntArray() throws Exception {
        final PojoEncoder<ArrayWrapper> pojoCodec = new PojoEncoder<ArrayWrapper>(Codecs.createDefault());
        //  1,056,227 ops per second
        //  1,223,540 ops per second with cache
        // 12,318,029 ops per second with simple ClassModel
        final ArrayWrapper pojo = new ArrayWrapper(new int[]{1, 2, 3});

        encodePojo(NUMBER_OF_TIMES_FOR_WARMUP, pojoCodec, pojo);

        for (int i = 0; i < 3; i++) {
            final long startTime = System.nanoTime();
            encodePojo(NUMBER_OF_TIMES_TO_RUN, pojoCodec, pojo);
            final long endTime = System.nanoTime();

            outputResults(startTime, endTime);
            testCleanup();
        }
    }

    @Test
    public void outputPerformanceForListOfPrimitives() throws Exception {
        final PojoEncoder<ListWrapper> pojoCodec = new PojoEncoder<ListWrapper>(Codecs.createDefault());
        //  890,847 ops per second
        //  945,522 ops per second with cache
        //5,058,040 ops per second with ClassModel
        final ListWrapper pojo = new ListWrapper(Arrays.asList(1, 2, 3));

        encodePojo(NUMBER_OF_TIMES_FOR_WARMUP, pojoCodec, pojo);

        for (int i = 0; i < 3; i++) {
            final long startTime = System.nanoTime();
            encodePojo(NUMBER_OF_TIMES_TO_RUN, pojoCodec, pojo);
            final long endTime = System.nanoTime();

            outputResults(startTime, endTime);
            testCleanup();
        }
    }

    @Test
    public void outputPerformanceForSimpleMap() throws Exception {
        final PojoEncoder<MapWrapper> pojoCodec = new PojoEncoder<MapWrapper>(Codecs.createDefault());
        //  934,809 ops per second
        //7,813,960 ops per second ith simple CLassModel
        final MapWrapper pojo = new MapWrapper();
        final Map<String, String> map = new HashMap<String, String>();
        map.put("field1", "field 1 value");
        pojo.setTheMap(map);

        encodePojo(NUMBER_OF_TIMES_FOR_WARMUP, pojoCodec, pojo);

        for (int i = 0; i < 3; i++) {
            final long startTime = System.nanoTime();
            encodePojo(NUMBER_OF_TIMES_TO_RUN, pojoCodec, pojo);
            final long endTime = System.nanoTime();

            outputResults(startTime, endTime);
            testCleanup();
        }
    }

    private void outputResults(final long startTime, final long endTime) {
        final long timeTakenInNanos = endTime - startTime;
        System.out.println(format("Test took: %,d ns", timeTakenInNanos));
        System.out.println(format("Test took: %,.3f seconds", timeTakenInNanos / NUMBER_OF_NANO_SECONDS_IN_A_SECOND));
        System.out.println(format("%,.0f ops per second%n", calculateOperationsPerSecond(timeTakenInNanos,
                                                                                         NUMBER_OF_TIMES_TO_RUN)));
    }

    private <T> void encodePojo(final int numberOfTimesForWarmup, final PojoEncoder<T> pojoCodec, final T pojo) {
        for (int i = 0; i < numberOfTimesForWarmup; i++) {
            pojoCodec.encode(bsonWriter, pojo);
        }
    }

}
