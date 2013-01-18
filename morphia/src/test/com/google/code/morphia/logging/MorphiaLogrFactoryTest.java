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
package com.google.code.morphia.logging;

import com.google.code.morphia.TestBase;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author us@thomas-daily.de
 */
public class MorphiaLogrFactoryTest extends TestBase {

    static {

        MorphiaLoggerFactory.reset();
        MorphiaLoggerFactory.registerLogger(TestLoggerFactory.class);
    }

    public MorphiaLogrFactoryTest() {

    }

    @Test
    public void testChoice() throws Exception {
        final Logr logr = MorphiaLoggerFactory.get(Object.class);
        final String className = logr.getClass().getName();
        Assert.assertTrue(className.startsWith(TestLoggerFactory.class.getName() + "$"));
    }

    @Override
    public void tearDown() {
        MorphiaLoggerFactory.reset();
        super.tearDown();
    }

    static class TestLoggerFactory implements LogrFactory {
        public Logr get(final Class<?> c) {
            return new Logr() {
                private static final long serialVersionUID = 1L;

                public void warning(final String msg, final Throwable t) {

                }

                public void warning(final String format, final Object... arg) {

                }

                public void warning(final String msg) {

                }

                public void trace(final String msg, final Throwable t) {

                }

                public void trace(final String format, final Object... arg) {

                }

                public void trace(final String msg) {

                }

                public boolean isWarningEnabled() {

                    return false;
                }

                public boolean isTraceEnabled() {

                    return false;
                }

                public boolean isInfoEnabled() {

                    return false;
                }

                public boolean isErrorEnabled() {

                    return false;
                }

                public boolean isDebugEnabled() {

                    return false;
                }

                public void info(final String msg, final Throwable t) {

                }

                public void info(final String format, final Object... arg) {

                }

                public void info(final String msg) {

                }

                public void error(final String msg, final Throwable t) {

                }

                public void error(final String format, final Object... arg) {

                }

                public void error(final String msg) {

                }

                public void debug(final String msg, final Throwable t) {

                }

                public void debug(final String format, final Object... arg) {

                }

                public void debug(final String msg) {

                }
            };
        }

    }
}
