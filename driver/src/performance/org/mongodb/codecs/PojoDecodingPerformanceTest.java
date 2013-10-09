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

import org.bson.BSONBinaryReader;
import org.bson.BSONBinaryWriter;
import org.bson.io.BasicInputBuffer;
import org.bson.io.BasicOutputBuffer;
import org.junit.Test;
import org.mongodb.codecs.pojo.Address;
import org.mongodb.codecs.pojo.EmptyPojo;
import org.mongodb.codecs.pojo.IntWrapper;
import org.mongodb.codecs.pojo.ListWrapper;
import org.mongodb.codecs.pojo.MapWrapper;
import org.mongodb.codecs.pojo.Name;
import org.mongodb.codecs.pojo.Person;
import org.mongodb.codecs.pojo.PojoWrapper;
import org.mongodb.codecs.pojo.StringWrapper;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static org.mongodb.codecs.PerfTestUtils.NUMBER_OF_NANO_SECONDS_IN_A_SECOND;
import static org.mongodb.codecs.PerfTestUtils.calculateOperationsPerSecond;
import static org.mongodb.codecs.PerfTestUtils.testCleanup;

public class PojoDecodingPerformanceTest {
    private static final int NUMBER_OF_TIMES_FOR_WARMUP = 10000;
    private static final int NUMBER_OF_TIMES_TO_RUN = 100000000;

    @Test
    public void outputBaselinePerformanceForPojoWithNoFields() throws Exception {
        final PojoCodec<EmptyPojo> pojoCodec = new PojoCodec<EmptyPojo>(Codecs.createDefault(), EmptyPojo.class);
        // 15,042,527 ops per second - faster than an empty document (9,223,774), which is odd....
        final EmptyPojo pojoToRead = new EmptyPojo();
        final byte[] emptyDocumentAsByteArrayForReader = gatherTestData(pojoToRead, pojoCodec).toByteArray();

        //warmup
        decodeDocument(NUMBER_OF_TIMES_FOR_WARMUP, emptyDocumentAsByteArrayForReader, pojoCodec);

        //test run
        for (int i = 0; i < 3; i++) {
            final long startTime = System.nanoTime();
            decodeDocument(NUMBER_OF_TIMES_TO_RUN, emptyDocumentAsByteArrayForReader, pojoCodec);
            final long endTime = System.nanoTime();

            outputResults(startTime, endTime);
            testCleanup();
        }
    }

    @Test
    public void outputPerformanceForAPojoWithASingleIntField() throws Exception {
        // 3,321,071 ops per second ops per second for Document
        final PojoCodec<IntWrapper> pojoCodec = new PojoCodec<IntWrapper>(Codecs.createDefault(), IntWrapper.class);
        // 1,889,418 ops per second, an order of magnitude slower than empty doc
        // 3,313,189 ops per second when caching results of getDeclaredField, similar to Document decoding
        final IntWrapper pojoToRead = new IntWrapper(34);
        final byte[] documentAsByteArrayForReader = gatherTestData(pojoToRead, pojoCodec).toByteArray();

        decodeDocument(NUMBER_OF_TIMES_FOR_WARMUP, documentAsByteArrayForReader, pojoCodec);

        for (int i = 0; i < 3; i++) {
            final long startTime = System.nanoTime();
            decodeDocument(NUMBER_OF_TIMES_TO_RUN, documentAsByteArrayForReader, pojoCodec);
            final long endTime = System.nanoTime();

            outputResults(startTime, endTime);
            testCleanup();
        }
    }

    @Test
    public void outputPerformanceForAPojoWithASingleStringField() throws Exception {
        //2,714,063 ops per second ops per second for Document
        final PojoCodec<StringWrapper> pojoCodec = new PojoCodec<StringWrapper>(Codecs.createDefault(), StringWrapper.class);
        // 1,568,890 ops per second
        // 2,547,603 ops per second with caching
        final StringWrapper documentToRead = new StringWrapper("theValue");
        final byte[] documentAsByteArrayForReader = gatherTestData(documentToRead, pojoCodec).toByteArray();
        decodeDocument(NUMBER_OF_TIMES_FOR_WARMUP, documentAsByteArrayForReader, pojoCodec);

        for (int i = 0; i < 3; i++) {
            final long startTime = System.nanoTime();
            decodeDocument(NUMBER_OF_TIMES_TO_RUN, documentAsByteArrayForReader, pojoCodec);
            final long endTime = System.nanoTime();

            outputResults(startTime, endTime);
            testCleanup();
        }
    }

    @Test
    public void outputPerformanceForListOfPrimitives() throws Exception {
        final PojoCodec<ListWrapper> pojoCodec = new PojoCodec<ListWrapper>(Codecs.createDefault(), ListWrapper.class);
        //   226,317 ops per second
        // 1,775,260 ops per second with caching of getDeclaredField
        final ListWrapper pojoToRead = new ListWrapper(Arrays.asList(1, 2, 3));

        final byte[] documentAsByteArrayForReader = gatherTestData(pojoToRead, pojoCodec).toByteArray();
        decodeDocument(NUMBER_OF_TIMES_FOR_WARMUP, documentAsByteArrayForReader, pojoCodec);

        for (int i = 0; i < 3; i++) {
            final long startTime = System.nanoTime();
            decodeDocument(NUMBER_OF_TIMES_TO_RUN, documentAsByteArrayForReader, pojoCodec);
            final long endTime = System.nanoTime();

            outputResults(startTime, endTime);
            testCleanup();
        }
    }

    @Test
    public void outputPerformanceForSimpleMap() throws Exception {
        //1,580,562 ops per second for Document
        final PojoCodec<MapWrapper> pojoCodec = new PojoCodec<MapWrapper>(Codecs.createDefault(), MapWrapper.class);
        //   163,350 ops per second
        // 1,460,819 ops per second with caching
        final MapWrapper pojoToRead = new MapWrapper();
        final Map<String, String> map = new HashMap<String, String>();
        map.put("field1", "field 1 value");
        pojoToRead.setTheMap(map);

        final byte[] documentAsByteArrayForReader = gatherTestData(pojoToRead, pojoCodec).toByteArray();
        decodeDocument(NUMBER_OF_TIMES_FOR_WARMUP, documentAsByteArrayForReader, pojoCodec);

        for (int i = 0; i < 3; i++) {
            final long startTime = System.nanoTime();
            decodeDocument(NUMBER_OF_TIMES_TO_RUN, documentAsByteArrayForReader, pojoCodec);
            final long endTime = System.nanoTime();

            outputResults(startTime, endTime);
            testCleanup();
        }
    }

    @Test
    public void outputPerformanceForPojoContainingAnotherPojo() throws Exception {
        final PojoCodec<PojoWrapper> pojoCodec = new PojoCodec<PojoWrapper>(Codecs.createDefault(), PojoWrapper.class);
        //   804,983 ops per second
        // 1,383,944 ops per second with caching
        final PojoWrapper pojoToRead = new PojoWrapper(new StringWrapper("the value"));

        final byte[] documentAsByteArrayForReader = gatherTestData(pojoToRead, pojoCodec).toByteArray();
        decodeDocument(NUMBER_OF_TIMES_FOR_WARMUP, documentAsByteArrayForReader, pojoCodec);

        for (int i = 0; i < 3; i++) {
            final long startTime = System.nanoTime();
            decodeDocument(NUMBER_OF_TIMES_TO_RUN, documentAsByteArrayForReader, pojoCodec);
            final long endTime = System.nanoTime();

            outputResults(startTime, endTime);
            testCleanup();
        }
    }

    @Test
    public void outputPerformanceForPojoContainingTwoPojo() throws Exception {
        final PojoCodec<Person> pojoCodec = new PojoCodec<Person>(Codecs.createDefault(), Person.class);
        // 303,042 ops per second
        // 490,159 ops per second not a massive improvement
        final Person pojoToRead = new Person(new Address(), new Name());

        final byte[] documentAsByteArrayForReader = gatherTestData(pojoToRead, pojoCodec).toByteArray();
        decodeDocument(NUMBER_OF_TIMES_FOR_WARMUP, documentAsByteArrayForReader, pojoCodec);

        for (int i = 0; i < 3; i++) {
            final long startTime = System.nanoTime();
            decodeDocument(NUMBER_OF_TIMES_TO_RUN, documentAsByteArrayForReader, pojoCodec);
            final long endTime = System.nanoTime();

            outputResults(startTime, endTime);
            testCleanup();
        }
    }

    private <T> BasicOutputBuffer gatherTestData(final T pojo, final PojoCodec<T> pojoCodec) {
        final BasicOutputBuffer buffer = new BasicOutputBuffer();
        final BSONBinaryWriter writer = new BSONBinaryWriter(buffer, false);

        try {
            pojoCodec.encode(writer, pojo);
            return buffer;
        } finally {
            writer.close();
        }
    }

    private void outputResults(final long startTime, final long endTime) {
        final long timeTakenInNanos = endTime - startTime;
        System.out.println(format("Test took: %,d ns", timeTakenInNanos));
        System.out.println(format("Test took: %,.3f seconds", timeTakenInNanos / NUMBER_OF_NANO_SECONDS_IN_A_SECOND));
        System.out.println(format("%,.0f ops per second%n", calculateOperationsPerSecond(timeTakenInNanos,
                                                                                         NUMBER_OF_TIMES_TO_RUN)));
    }

    private <T> void decodeDocument(final int timesToRun, final byte[] inputAsByteArray, final PojoCodec<T> pojoCodec) {
        for (int i = 0; i < timesToRun; i++) {
            final BSONBinaryReader reader = new BSONBinaryReader(new BasicInputBuffer(ByteBuffer.wrap(inputAsByteArray)), true);
            try {
                pojoCodec.decode(reader);
            } finally {
                reader.close();
            }
        }
    }

}
