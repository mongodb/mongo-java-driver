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

package com.google.code.morphia.logging;

import com.google.code.morphia.logging.jdk.JDKLoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class MorphiaLoggerFactory {
    private static LogrFactory loggerFactory = null;

    private static final List<String> FACTORIES = new ArrayList(Arrays.asList(JDKLoggerFactory.class.getName(),
                                                                             "com.google.code.morphia.logging.slf4j"
                                                                             + ".SLF4JLogrImplFactory"));

    private static synchronized void init() {
        if (MorphiaLoggerFactory.loggerFactory == null) {
            chooseLoggerFactory();
        }
    }

    private static void chooseLoggerFactory() {
        for (final String f : MorphiaLoggerFactory.FACTORIES) {
            MorphiaLoggerFactory.loggerFactory = newInstance(f);
            if (MorphiaLoggerFactory.loggerFactory != null) {
                loggerFactory.get(MorphiaLoggerFactory.class)
                             .info("LoggerImplFactory set to " + loggerFactory.getClass().getName());
                return;
            }
        }
        throw new IllegalStateException("Cannot instanciate any MorphiaLoggerFactory");
    }

    private static LogrFactory newInstance(final String f) {
        try {
            final Class<?> c = Class.forName(f);
            return (LogrFactory) c.newInstance();
        } catch (Throwable ignore) {
        }
        return null;
    }

    public static Logr get(final Class<?> c) {
        init();
        return MorphiaLoggerFactory.loggerFactory.get(c);
    }

    /**
     * Register a LoggerFactory; last one registered is used. *
     */
    public static void registerLogger(final Class<? extends LogrFactory> factoryClass) {
        if (MorphiaLoggerFactory.loggerFactory == null) {
            MorphiaLoggerFactory.FACTORIES.add(0, factoryClass.getName());
        }
        else {
            throw new IllegalStateException("LoggerImplFactory must be registered before logging is initialized.");
        }
    }

    public static void reset() {
        loggerFactory = null;
    }
}
