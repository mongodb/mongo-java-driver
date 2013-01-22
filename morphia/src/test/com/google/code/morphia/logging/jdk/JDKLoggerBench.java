/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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

/**
 *
 */
package com.google.code.morphia.logging.jdk;

import com.google.code.morphia.logging.Logr;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JDKLoggerBench implements I {
    private static Logr logger;
    private static final JDKLoggerBench LOGGER_BENCH = new JDKLoggerBench();

    public static void main(final String[] args) {

        System.err.println("When Log-Level is beyond interest\n---------------------------------------------");
        Logger.getLogger(JDKLoggerBench.class.getName()).setLevel(Level.WARNING);
        System.setOut(new PrintStream(new OutputStream() {

            @Override
            public void write(final int b) throws IOException {

            }
        }));

        logger = new JDKLogger(JDKLoggerBench.class);
        bench();
        logger = new FasterJDKLogger(JDKLoggerBench.class);
        bench();
        logger = new FastestJDKLogger(JDKLoggerBench.class);
        bench();

        System.err.println("\nWhen Log-Level is within interest\n---------------------------------------------");
        Logger.getLogger(JDKLoggerBench.class.getName()).setLevel(Level.FINEST);

        logger = new JDKLogger(JDKLoggerBench.class);
        bench();
        logger = new FasterJDKLogger(JDKLoggerBench.class);
        bench();
        logger = new FastestJDKLogger(JDKLoggerBench.class);
        bench();

    }

    private static void calm() {
        System.gc();
        Thread.yield();
        try {
            Thread.sleep(5000);
        } catch (final InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private static void bench() {
        final long start;
        final long end;
        System.err.println("\nUsing " + logger.getClass());
        System.err.println("Warmup");
        for (int j = 0; j < 10000; j++) {
            LOGGER_BENCH.foo();
        }
        calm();

        System.err.println("Start");
        start = System.currentTimeMillis();
        for (int j = 0; j < 1000000; j++) {
            LOGGER_BENCH.foo();
        }
        end = System.currentTimeMillis();
        System.err.println("RT " + ((end - start)) + "ms");
    }

    public void foo() {
        JDKLoggerBench.logger.debug("buh");
    }

}

interface I {
    void foo();
}