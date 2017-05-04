/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.bson.json;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQuery;
import java.util.Calendar;

import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;

final class DateTimeFormatter {
    private static final FormatterImpl FORMATTER_IMPL;

    static {
        FormatterImpl dateTimeHelper;
        try {
            dateTimeHelper = loadDateTimeFormatter("org.bson.json.DateTimeFormatter$Java8DateTimeFormatter");
        } catch (LinkageError e) {
            // this is expected if running on a release prior to Java 8: fallback to JAXB.
            dateTimeHelper = loadDateTimeFormatter("org.bson.json.DateTimeFormatter$JaxbDateTimeFormatter");
        }

        FORMATTER_IMPL = dateTimeHelper;
    }

    private static FormatterImpl loadDateTimeFormatter(final String className) {
        try {
            return (FormatterImpl) Class.forName(className).newInstance();
        } catch (ClassNotFoundException e) {
            // this is unexpected as it means the class itself is not found
            throw new ExceptionInInitializerError(e);
        } catch (InstantiationException e) {
            // this is unexpected as it means the class can't be instantiated
            throw new ExceptionInInitializerError(e);
        } catch (IllegalAccessException e) {
            // this is unexpected as it means the no-args constructor isn't accessible
            throw new ExceptionInInitializerError(e);
        }
    }

    static long parse(final String dateTimeString) {
        return FORMATTER_IMPL.parse(dateTimeString);
    }

    private interface FormatterImpl {
        long parse(String dateTimeString);
    }

    // Reflective use of DatatypeConverter avoids a compile-time dependency on the java.xml.bind module in Java 9
    static class JaxbDateTimeFormatter implements FormatterImpl {

        private static final Method DATATYPE_CONVERTER_PARSE_DATE_TIME_METHOD;

        static {
            try {
                DATATYPE_CONVERTER_PARSE_DATE_TIME_METHOD = Class.forName("javax.xml.bind.DatatypeConverter")
                                                                    .getDeclaredMethod("parseDateTime", String.class);
            } catch (NoSuchMethodException e) {
                throw new ExceptionInInitializerError(e);
            } catch (ClassNotFoundException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        @Override
        public long parse(final String dateTimeString) {
            try {
                return ((Calendar) DATATYPE_CONVERTER_PARSE_DATE_TIME_METHOD.invoke(null, dateTimeString)).getTimeInMillis();
            } catch (IllegalAccessException e) {
                throw new IllegalStateException(e);
            } catch (InvocationTargetException e) {
                throw (RuntimeException) e.getCause();
            }
        }
    }

    static class Java8DateTimeFormatter implements FormatterImpl {

        // if running on Java 8 or above then java.time.format.DateTimeFormatter will be available and initialization will succeed.
        // Otherwise it will fail.
        static {
            try {
                Class.forName("java.time.format.DateTimeFormatter");
            } catch (ClassNotFoundException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        @Override
        public long parse(final String dateTimeString) {
            try {
                return ISO_OFFSET_DATE_TIME.parse(dateTimeString, new TemporalQuery<Instant>() {
                    @Override
                    public Instant queryFrom(final TemporalAccessor temporal) {
                        return Instant.from(temporal);
                    }
                }).toEpochMilli();
            } catch (DateTimeParseException e) {
                throw new IllegalArgumentException(e.getMessage());
            }
        }
    }

    private DateTimeFormatter() {
    }
}
