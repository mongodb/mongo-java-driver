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

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static java.lang.String.format;
import static org.mongodb.codecs.PerfTestUtils.NUMBER_OF_NANO_SECONDS_IN_A_SECOND;
import static org.mongodb.codecs.PerfTestUtils.calculateOperationsPerSecond;
import static org.mongodb.codecs.PerfTestUtils.testCleanup;

public class InstanceOfPerformanceTest {
    private static final int NUMBER_OF_TIMES_FOR_WARMUP = 10000;
    private static final int NUMBER_OF_TIMES_TO_RUN = 100000000;

    private static final List<String> LIST = Arrays.asList("1", "2", "3");

    private static final boolean[] RESULTS = new boolean[NUMBER_OF_TIMES_TO_RUN];

    @Test
    public void outputPerformanceForArrayList() throws Exception {
        doInstanceOf(NUMBER_OF_TIMES_FOR_WARMUP, LIST);

        for (int i = 0; i < 3; i++) {
            doPerformanceRun(LIST);
        }
    }

    @Test
    public void outputPerformanceForArray() throws Exception {
        final String[] stringArray = {"1", "2", "3"};
        doInstanceOf(NUMBER_OF_TIMES_FOR_WARMUP, stringArray);

        for (int i = 0; i < 3; i++) {
            doPerformanceRun(stringArray);
        }
    }

    @Test
    public void outputPerformanceForMixOfObjects() throws Exception {
        doInstanceOf(NUMBER_OF_TIMES_FOR_WARMUP, LIST);

        doPerformanceRun(LIST);
        doPerformanceRun(new String[]{"1", "2", "3"});
        doPerformanceRun(LIST);
        doPerformanceRun(new String[]{"1", "2", "3"});
        doPerformanceRun(LIST);
        doPerformanceRun(new String[]{"1", "2", "3"});
        doPerformanceRun("Thingie");
        doPerformanceRun(new String[]{"1", "2", "3"});
    }

    private void doPerformanceRun(final Object object) throws InterruptedException {
        long startTime = System.nanoTime();
        doInstanceOf(NUMBER_OF_TIMES_TO_RUN, object);
        long endTime = System.nanoTime();

        outputResults(startTime, endTime);
        testCleanup();
    }

    private void doInstanceOf(final int numberOfIterations, final Object list) {
        for (int i = numberOfIterations; i != 0; i--) {
            RESULTS[i - 1] = doInstanceOf(list);
        }
    }

    @SuppressWarnings("ImplicitArrayToString")
    private void outputResults(final long startTime, final long endTime) {
        final long timeTakenInNanos = endTime - startTime;
        System.out.println(RESULTS); // we need to do this so the compiler doesn't optimise away the whole test
        System.out.println(format("Test took: %,d ns", timeTakenInNanos));
        System.out.println(format("Test took: %,.3f seconds", timeTakenInNanos / NUMBER_OF_NANO_SECONDS_IN_A_SECOND));
        System.out.println(format("%,.0f ops per second%n", calculateOperationsPerSecond(timeTakenInNanos,
                                                                                         NUMBER_OF_TIMES_TO_RUN)));
    }

    //The thing we're actually testing
    private boolean doInstanceOf(final Object object) {
        return object instanceof Iterable;
    }

}
