/*
 * Copyright 2008-present MongoDB, Inc.
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
package com.mongodb.internal.connection.debug;

import com.mongodb.annotations.ThreadSafe;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicMarkableReference;

import static com.mongodb.assertions.Assertions.assertFalse;
import static com.mongodb.assertions.Assertions.assertTrue;

public final class Debugger {
    /**
     * A logger for debugging I/O problems. The name of this logger is {@code "org.mongodb.driver.connection.debugger"}.
     */
    static final Logger LOGGER = Loggers.getLogger("connection.debugger");
    /**
     * <code>{@value #DEBUGGER_SYSPROP_NAME}</code>.
     */
    static final String DEBUGGER_SYSPROP_NAME = "org.mongodb.driver.connection.debugger";
    /**
     * A universally unique ID. Usually represents a single run of an application, unless the application uses multiple class loaders
     * each defining {@link Debugger}, which in turn may result in generating
     * multiple IDs per single run of an application—one per initialization of each defined class.
     */
    static final UUID RUN_ID = UUID.randomUUID();
    /**
     * The string <code>{@value #FAKE_LINE_SEPARATOR}</code> (6 characters)
     * representing a Unicode code point for {@code LINE SEPARATOR}.
     * May be used, for example, to encode a stack trace as a single-line {@link String}.
     */
    static final String FAKE_LINE_SEPARATOR = "\\u2028";
    /**
     * Is used to
     * <ul>
     *     <li>log once when reporting mode changes;</li>
     *     <li>store the {@linkplain #useReportingMode(ReportingMode) overriding mode}
     *     (overriding values are {@linkplain AtomicMarkableReference#isMarked() marked}).</li>
     * </ul>
     *
     */
    private static final AtomicMarkableReference<ReportingMode> PREV_REPORTING_MODE = new AtomicMarkableReference<>(
            ReportingMode.OFF, false);

    /**
     * Returns as specified below, unless the reporting mode is {@linkplain #useReportingMode(ReportingMode) overridden}:
     * <ul>
     *     <li>{@link ReportingMode#LOG_AND_THROW} iff the value of the {@linkplain System#getProperty(String) system property} with name
     *     {@link #DEBUGGER_SYSPROP_NAME} is {@code "LOG_AND_THROW"};</li>
     *     <li>{@link ReportingMode#LOG} iff both of the following is true: does not return {@link ReportingMode#LOG_AND_THROW} and
     *     the logger named {@code "org.mongodb.driver.connection.debugger"} has {@linkplain Logger#isDebugEnabled() DEBUG enabled};</li>
     *     <li>{@link ReportingMode#OFF} otherwise.</li>
     * </ul>
     */
    static ReportingMode reportingMode() {
        boolean[] observedOverriding = new boolean[1];
        ReportingMode observedMode;
        ReportingMode mode;
        do {
            observedMode = PREV_REPORTING_MODE.get(observedOverriding);
            if (observedOverriding[0]) {
                return observedMode;
            } else if (System.getProperty("org.mongodb.driver.connection.debugger", ReportingMode.OFF.name())
                    .equals(ReportingMode.LOG_AND_THROW.name())) {
                mode = ReportingMode.LOG_AND_THROW;
            } else if (LOGGER.isDebugEnabled()) {
                mode = ReportingMode.LOG;
            } else {
                mode = ReportingMode.OFF;
            }
        } while (!tryRegister(observedMode, mode, false, false));
        return mode;
    }

    /**
     * Overrides the reporting mode rules specified by {@link #reportingMode()} and makes {@link #reportingMode()} return the
     * specified {@code mode}. Overriding modes cannot exist concurrently,
     * so one has to be {@linkplain OverridingReportingMode#remove() removed} before setting another one.
     */
    public static OverridingReportingMode useReportingMode(final ReportingMode mode) {
        ReportingMode observedMode;
        boolean[] observedOverriding = new boolean[1];
        do {
            observedMode = PREV_REPORTING_MODE.get(observedOverriding);
        } while (!tryRegister(observedMode, mode,
                // concurrently existing overriding values are not allowed
                assertFalse(observedOverriding[0]),
                true));
        return new OverridingReportingMode(mode);
    }

    /**
     * Encodes the specified {@link Throwable} as a single-line {@link String} by using {@link #FAKE_LINE_SEPARATOR}.
     */
    static String toString(final Throwable t) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw, false);
        t.printStackTrace(pw);
        pw.flush();
        return sw.toString().replace(System.lineSeparator(), FAKE_LINE_SEPARATOR);
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private static boolean tryRegister(final ReportingMode expectedMode, final ReportingMode mode,
            final boolean expectedOverriding, final boolean overriding) {
        if (expectedMode == mode && expectedOverriding == overriding) {
            return true;
        } else if (PREV_REPORTING_MODE.compareAndSet(expectedMode, mode, expectedOverriding, overriding)) {
            if (expectedMode != mode) {
                LOGGER.info("Reporting mode changed from " + expectedMode + " to " + mode
                        + (mode == ReportingMode.LOG_AND_THROW ? " (must not be used in production)" : ""));
            }
            return true;
        } else {
            return false;
        }
    }

    private Debugger() {
        throw new AssertionError();
    }

    /**
     * See {@link #reportingMode()} for the documentation on how to change the mode.
     */
    public enum ReportingMode {
        /**
         * Is an extension of {@link #LOG}, may affect the behavior of the driver. Must not be used in production or by external users.
         */
        LOG_AND_THROW,
        /**
         * Debugging is on and has no effect on behavior of the driver. It is intended that in this mode debugging has negligible
         * effect on the performance characteristics of the driver, but may affect stack traces and logging.
         */
        LOG,
        /**
         * Debugging is off and has no effect. This is the default.
         */
        OFF;

        /**
         * Returns {@code true} iff {@code this} is not {@link #OFF}.
         */
        boolean on() {
            return this != OFF;
        }
    }

    @ThreadSafe
    public static final class OverridingReportingMode implements AutoCloseable {
        private final ReportingMode mode;
        private final AtomicBoolean removed;

        OverridingReportingMode(final ReportingMode mode) {
            this.mode = mode;
            removed = new AtomicBoolean();
        }

        public ReportingMode reportingMode() {
            return mode;
        }

        /**
         * Removes the {@linkplain #useReportingMode(ReportingMode) overriding reporting mode}.
         *
         * @see #remove()
         */
        @Override
        public void close() {
            remove();
        }

        /**
         * @see #close()
         */
        public void remove() {
            if (!removed.getAndSet(true)) {
                assertTrue(PREV_REPORTING_MODE.attemptMark(mode, false));
                // log the reporting mode change if any
                Debugger.reportingMode();
            }
        }

        @Override
        public String toString() {
            return "OverridingReportingMode{"
                    + "mode=" + mode
                    + ", removed=" + removed
                    + '}';
        }
    }
}
