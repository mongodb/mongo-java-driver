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
import org.bson.BSONBinaryWriterSettings;
import org.bson.BSONWriterSettings;
import org.bson.io.BasicInputBuffer;
import org.bson.io.BasicOutputBuffer;
import org.bson.types.CodeWithScope;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.Document;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static org.mongodb.codecs.PerfTestUtils.NUMBER_OF_NANO_SECONDS_IN_A_SECOND;
import static org.mongodb.codecs.PerfTestUtils.calculateOperationsPerSecond;
import static org.mongodb.codecs.PerfTestUtils.testCleanup;

public class DocumentDecodingPerformanceTest {
    private static final int NUMBER_OF_TIMES_FOR_WARMUP = 10000;
    private static final int NUMBER_OF_TIMES_TO_RUN = 100000000;
    private DocumentCodec documentCodec;

    @Before
    public void setUp() throws Exception {
        documentCodec = new DocumentCodec(PrimitiveCodecs.createDefault());
    }

    @Test
    public void outputBaselinePerformanceForEmptyDocument() throws Exception {
        // 9,223,774 ops per second - about the same scale as encoding when encoding uses the real reader (10,837,117 ops per second)
        // 23,595,719 ops per second baseline (no decode) - so the constant creation of readers doesn't hurt us
        final Document documentToRead = new Document();
        final byte[] emptyDocumentAsByteArrayForReader = gatherTestData(documentToRead).toByteArray();

        //warmup
        decodeDocument(NUMBER_OF_TIMES_FOR_WARMUP, emptyDocumentAsByteArrayForReader);

        //test run
        for (int i = 0; i < 3; i++) {
            long startTime = System.nanoTime();
            decodeDocument(NUMBER_OF_TIMES_TO_RUN, emptyDocumentAsByteArrayForReader);
            long endTime = System.nanoTime();

            outputResults(startTime, endTime);
            testCleanup();
        }
    }

    @Test
    public void outputPerformanceForADocumentWithASingleIntField() throws Exception {
        //3,321,071 ops per second ops per second
        //for a single, primitive (int) field.  Not quite an order of magnitude slower than empty doc
        final Document documentToRead = new Document("anIntValue", 34);
        final byte[] documentAsByteArrayForReader = gatherTestData(documentToRead).toByteArray();

        decodeDocument(NUMBER_OF_TIMES_FOR_WARMUP, documentAsByteArrayForReader);

        for (int i = 0; i < 3; i++) {
            long startTime = System.nanoTime();
            decodeDocument(NUMBER_OF_TIMES_TO_RUN, documentAsByteArrayForReader);
            long endTime = System.nanoTime();

            outputResults(startTime, endTime);
            testCleanup();
        }
    }

    @Test
    public void outputPerformanceForADocumentWithASingleStringField() throws Exception {
        //2,714,063 ops per second ops per second
        //for a single, String field.  Nearly an order of magnitude slower than an empty doc, and slower than int
        final Document documentToRead = new Document("aString", "theValue");
        final byte[] documentAsByteArrayForReader = gatherTestData(documentToRead).toByteArray();
        decodeDocument(NUMBER_OF_TIMES_FOR_WARMUP, documentAsByteArrayForReader);

        for (int i = 0; i < 3; i++) {
            long startTime = System.nanoTime();
            decodeDocument(NUMBER_OF_TIMES_TO_RUN, documentAsByteArrayForReader);
            long endTime = System.nanoTime();

            outputResults(startTime, endTime);
            testCleanup();
        }
    }

    @Test
    public void outputPerformanceForIntArray() throws Exception {
        //1,929,484 ops per second
        //Same order of magnitude as a single field
        final Document documentToRead = new Document("theArray", new int[]{1, 2, 3});

        final byte[] documentAsByteArrayForReader = gatherTestData(documentToRead).toByteArray();
        decodeDocument(NUMBER_OF_TIMES_FOR_WARMUP, documentAsByteArrayForReader);

        for (int i = 0; i < 3; i++) {
            long startTime = System.nanoTime();
            decodeDocument(NUMBER_OF_TIMES_TO_RUN, documentAsByteArrayForReader);
            long endTime = System.nanoTime();

            outputResults(startTime, endTime);
            testCleanup();
        }
    }

    @Test
    public void outputPerformanceForListOfPrimitives() throws Exception {
        // 1,907,792 ops per second
        // not surprisingly the same sort of number as an array
        final Document documentToRead = new Document("theArray", Arrays.asList(1, 2, 3));

        final byte[] documentAsByteArrayForReader = gatherTestData(documentToRead).toByteArray();
        decodeDocument(NUMBER_OF_TIMES_FOR_WARMUP, documentAsByteArrayForReader);

        for (int i = 0; i < 3; i++) {
            long startTime = System.nanoTime();
            decodeDocument(NUMBER_OF_TIMES_TO_RUN, documentAsByteArrayForReader);
            long endTime = System.nanoTime();

            outputResults(startTime, endTime);
            testCleanup();
        }
    }

    @Test
    public void outputPerformanceForSimpleMap() throws Exception {
        //1,580,562 ops per second
        final Document documentToRead = new Document();
        final Map<String, String> map = new HashMap<String, String>();
        map.put("field1", "field 1 value");
        documentToRead.append("theMap", map);

        final byte[] documentAsByteArrayForReader = gatherTestData(documentToRead).toByteArray();
        decodeDocument(NUMBER_OF_TIMES_FOR_WARMUP, documentAsByteArrayForReader);

        for (int i = 0; i < 3; i++) {
            long startTime = System.nanoTime();
            decodeDocument(NUMBER_OF_TIMES_TO_RUN, documentAsByteArrayForReader);
            long endTime = System.nanoTime();

            outputResults(startTime, endTime);
            testCleanup();
        }
    }

    @Test
    public void outputPerformanceForNestedDocument() throws Exception {
        //1,552,114 ops per second
        //same sort of results as Map, not surprisingly
        final Document documentToRead = new Document();
        final Document subDocument = new Document("field1", "field 1 value");
        documentToRead.append("theSubDocument", subDocument);

        final byte[] documentAsByteArrayForReader = gatherTestData(documentToRead).toByteArray();
        decodeDocument(NUMBER_OF_TIMES_FOR_WARMUP, documentAsByteArrayForReader);

        for (int i = 0; i < 3; i++) {
            long startTime = System.nanoTime();
            decodeDocument(NUMBER_OF_TIMES_TO_RUN, documentAsByteArrayForReader);
            long endTime = System.nanoTime();

            outputResults(startTime, endTime);
            testCleanup();
        }
    }
    @Test
    public void outputPerformanceForCodeWithScope() throws Exception {
        //1,339,085 ops per second
        final Document documentToRead = new Document();
        final CodeWithScope codeWithScope = new CodeWithScope("the javascript", new Document("field1", "field 1 value"));
        documentToRead.append("theCodeWithScope", codeWithScope);

        final byte[] documentAsByteArrayForReader = gatherTestData(documentToRead).toByteArray();
        decodeDocument(NUMBER_OF_TIMES_FOR_WARMUP, documentAsByteArrayForReader);

        for (int i = 0; i < 3; i++) {
            long startTime = System.nanoTime();
            decodeDocument(NUMBER_OF_TIMES_TO_RUN, documentAsByteArrayForReader);
            long endTime = System.nanoTime();

            outputResults(startTime, endTime);
            testCleanup();
        }
    }

    private BasicOutputBuffer gatherTestData(final Document document) throws Exception {
        BasicOutputBuffer buffer = new BasicOutputBuffer();
        BSONBinaryWriter writer = new BSONBinaryWriter(new BSONWriterSettings(100),
                                                       new BSONBinaryWriterSettings(1024 * 1024), buffer);

        documentCodec.encode(writer, document);
        return buffer;
    }

    private void outputResults(final long startTime, final long endTime) {
        final long timeTakenInNanos = endTime - startTime;
        System.out.println(format("Test took: %,d ns", timeTakenInNanos));
        System.out.println(format("Test took: %,.3f seconds", timeTakenInNanos / NUMBER_OF_NANO_SECONDS_IN_A_SECOND));
        System.out.println(format("%,.0f ops per second%n", calculateOperationsPerSecond(timeTakenInNanos,
                                                                                         NUMBER_OF_TIMES_TO_RUN)));
    }

    private void decodeDocument(final int timesToRun, final byte[] inputAsByteArray) {
        for (int i = 0; i < timesToRun; i++) {
            documentCodec.decode(new BSONBinaryReader(new BasicInputBuffer(ByteBuffer.wrap(inputAsByteArray))));
        }
    }

}
