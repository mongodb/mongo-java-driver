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

import org.junit.Before;
import org.junit.Test;
import org.mongodb.Document;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.codecs.PrimitiveCodecs;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;

public class DocumentEncodingPerformanceTest {
    private static final int NUMBER_OF_TIMES_FOR_WARMUP = 10000;
    private static final int NUMBER_OF_TIMES_TO_RUN = 100000000;
    private DocumentCodec documentCodec;
    private StubBSONWriter bsonWriter;

    @Before
    public void setUp() throws Exception {
        documentCodec = new DocumentCodec(PrimitiveCodecs.createDefault());
        bsonWriter = new StubBSONWriter();
    }

    @Test
    public void outputBaselinePerformanceForEmptyDocument() throws Exception {
        //177,327,917 ops per second
        Document document = new Document();
        encodeDocument(NUMBER_OF_TIMES_FOR_WARMUP, document);

        for (int i = 0; i < 3; i++) {
            long startTime = System.nanoTime();
            encodeDocument(NUMBER_OF_TIMES_TO_RUN, document);
            long endTime = System.nanoTime();

            outputResults(startTime, endTime);
            PerfTestUtils.testCleanup();
        }
    }

    @Test
    public void outputPerformanceForADocumentWithASingleIntField() throws Exception {
        //13,106,251 ops per second
        //for a single, primitive (int) field.  An order of magnitude slower than an empty doc
        //33,437,167 ops per second when encoding goes straight to Codecs.  3x is not bad
        Document document = new Document("anIntValue", 34);
        encodeDocument(NUMBER_OF_TIMES_FOR_WARMUP, document);

        for (int i = 0; i < 3; i++) {
            long startTime = System.nanoTime();
            encodeDocument(NUMBER_OF_TIMES_TO_RUN, document);
            long endTime = System.nanoTime();

            outputResults(startTime, endTime);
            PerfTestUtils.testCleanup();
        }
    }

    @Test
    public void outputPerformanceForADocumentWithASingleStringField() throws Exception {
        //12,617,895 ops per second
        //for a single, String field.  An order of magnitude slower than an empty doc
        //32,443,547 ops per second when encoding delegated to codecs
        Document document = new Document("aString", "theValue");
        encodeDocument(NUMBER_OF_TIMES_FOR_WARMUP, document);

        for (int i = 0; i < 3; i++) {
            long startTime = System.nanoTime();
            encodeDocument(NUMBER_OF_TIMES_TO_RUN, document);
            long endTime = System.nanoTime();

            outputResults(startTime, endTime);
            PerfTestUtils.testCleanup();
        }
    }

    @Test
    public void outputPerformanceForIntArray() throws Exception {
        //1,500,839 ops per second, An order of magnitude slower than a primitive field
        //11,009,021 ops per second when you use the ArraysCodec
        //32,075,750 ops per second when you delegate directly to codecs with no instanceof
        Document document = new Document();
        document.append("theArray", new int[]{1, 2, 3});

        encodeDocument(NUMBER_OF_TIMES_FOR_WARMUP, document);

        for (int i = 0; i < 3; i++) {
            long startTime = System.nanoTime();
            encodeDocument(NUMBER_OF_TIMES_TO_RUN, document);
            long endTime = System.nanoTime();

            outputResults(startTime, endTime);
            PerfTestUtils.testCleanup();
        }
    }

    @Test
    public void outputPerformanceForListOfPrimitives() throws Exception {
        //3,945,723 ops per second original version.  Ouch.  Only marginally better than a primitive array
        //6,375,324 ops per second when you use the IterableCodec.  Not a massive improvement
        //7,700,641 ops per second when all encoding is delegated to Codecs.
        Document document = new Document();
        document.append("theArray", Arrays.asList(1, 2, 3));

        encodeDocument(NUMBER_OF_TIMES_FOR_WARMUP, document);

        for (int i = 0; i < 3; i++) {
            long startTime = System.nanoTime();
            encodeDocument(NUMBER_OF_TIMES_TO_RUN, document);
            long endTime = System.nanoTime();

            outputResults(startTime, endTime);
            PerfTestUtils.testCleanup();
        }
    }

    @Test
    public void outputPerformanceForSimpleMap() throws Exception {
        //7,789,176 ops per second initially - not much slower than a single primitive
        //10,171,206 ops per second when you use the MapsCodec - not much in it
        //10,186,710 ops per second with direct to Codecs encoding
        Document document = new Document();
        Map<String, String> map = new HashMap<String, String>();
        map.put("field1", "field 1 value");
        document.append("theMap", map);

        encodeDocument(NUMBER_OF_TIMES_FOR_WARMUP, document);

        for (int i = 0; i < 3; i++) {
            long startTime = System.nanoTime();
            encodeDocument(NUMBER_OF_TIMES_TO_RUN, document);
            long endTime = System.nanoTime();

            outputResults(startTime, endTime);
            PerfTestUtils.testCleanup();
        }
    }

    //CHECKSTYLE:OFF
    private void outputResults(final long startTime, final long endTime) {
        System.out.println(format("Number of names encoded: %d", bsonWriter.getNumberOfNamesWritten()));
        System.out.println(format("Number of ints encoded: %d", bsonWriter.getNumberOfIntsWritten()));
        System.out.println(format("Number of Strings encoded: %d", bsonWriter.getNumberOfStringsEncoded()));
        long timeTakenInNanos = endTime - startTime;
        System.out.println(format("Test took: %,d ns", timeTakenInNanos));
        System.out.println(String.format("Test took: %,.3f seconds", timeTakenInNanos / PerfTestUtils.NUMBER_OF_NANO_SECONDS_IN_A_SECOND));
        System.out.println(String.format("%,.0f ops per second%n", PerfTestUtils.calculateOperationsPerSecond(timeTakenInNanos,
                                                                                                              NUMBER_OF_TIMES_TO_RUN)));
    }
    //CHECKSTYLE:ON

    private void encodeDocument(final int numberOfTimesForWarmup, final Document document) {
        for (int i = 0; i < numberOfTimesForWarmup; i++) {
            documentCodec.encode(bsonWriter, document);
        }
    }

}
