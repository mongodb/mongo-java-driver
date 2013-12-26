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
import org.mongodb.codecs.IntegerCodec;
import org.mongodb.codecs.PrimitiveCodecs;

import static java.lang.String.format;
import static org.mongodb.performance.codecs.PerfTestUtils.NUMBER_OF_NANO_SECONDS_IN_A_SECOND;
import static org.mongodb.performance.codecs.PerfTestUtils.testCleanup;

public class IntegerArrayCodecPerformanceTest {
    private static final int NUMBER_OF_TIMES_FOR_WARMUP = 10000;
    private static final int NUMBER_OF_TIMES_TO_RUN = 100000000;

    private StubBSONWriter bsonWriter;
    private final IntegerCodec integerCodec = new IntegerCodec();
    private final PrimitiveCodecs primitiveCodecs = PrimitiveCodecs.createDefault();

    @Before
    public void setUp() throws Exception {
        bsonWriter = new StubBSONWriter();
    }

    @Test
    public void outputPerformanceForUsingIntegerCodecDirectly() throws Exception {
        int[] intArrayToEncode = {1, 2, 3};
        encodeArrayUsingIntegerCodecDirectly(intArrayToEncode, NUMBER_OF_TIMES_FOR_WARMUP);

        for (int i = 0; i < 3; i++) {
            long startTime = System.nanoTime();
            encodeArrayUsingIntegerCodecDirectly(intArrayToEncode, NUMBER_OF_TIMES_TO_RUN);
            long endTime = System.nanoTime();

            outputResults(startTime, endTime);
            testCleanup();
        }
    }

    @Test
    public void outputPerformanceForEncodingViaPrimitiveCodecs() throws Exception {
        int[] intArrayToEncode = {1, 2, 3};
        encodeArrayUsingPrimitiveCodecs(intArrayToEncode, NUMBER_OF_TIMES_FOR_WARMUP);

        for (int i = 0; i < 3; i++) {
            long startTime = System.nanoTime();
            encodeArrayUsingPrimitiveCodecs(intArrayToEncode, NUMBER_OF_TIMES_TO_RUN);
            long endTime = System.nanoTime();

            outputResults(startTime, endTime);
            testCleanup();
        }
    }

    //CHECKSTYLE:OFF
    private void outputResults(final long startTime, final long endTime) {
        System.out.println(format("Written: %d", bsonWriter.getNumberOfIntsWritten()));
        long timeTakenInNanos = endTime - startTime;
        System.out.println(format("Test took: %,d ns", timeTakenInNanos));
        System.out.println(format("Test took: %,.3f seconds", timeTakenInNanos / PerfTestUtils.NUMBER_OF_NANO_SECONDS_IN_A_SECOND));
        System.out.println(format("%,.0f ops per second%n", calculateOperationsPerSecond(timeTakenInNanos)));
    }
    //CHECKSTYLE:ON

    private double calculateOperationsPerSecond(final long timeTakenInNanos) {
        return (NUMBER_OF_NANO_SECONDS_IN_A_SECOND / timeTakenInNanos) * NUMBER_OF_TIMES_TO_RUN;
    }

    // Actual implementations we're testing

    //Here we're testing using the Integer Codec directly
    private void encodeArrayUsingIntegerCodecDirectly(final int[] intArrayToEncode, final int numberOfIterations) {
        //97,384,164 ops per second using the correct codec directly
        for (int i = numberOfIterations; i != 0; i--) {
            for (int intValue : intArrayToEncode) {
                integerCodec.encode(bsonWriter, intValue);
            }
        }
    }

    //Here we're testing using the Integer Codec via PrimitiveCodecs and the type lookup involved in that
    private void encodeArrayUsingPrimitiveCodecs(final int[] intArrayToEncode, final int numberOfIterations) {
        //28,653,139 ops per second when you have the lookup via PrimitiveCodecs
        for (int i = numberOfIterations; i != 0; i--) {
            for (int intValue : intArrayToEncode) {
                primitiveCodecs.encode(bsonWriter, intValue);
            }
        }
    }

}
