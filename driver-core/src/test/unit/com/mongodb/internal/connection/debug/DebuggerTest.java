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

import com.mongodb.internal.connection.debug.Debugger.OverridingReportingMode;
import com.mongodb.internal.connection.debug.Debugger.ReportingMode;
import com.mongodb.lang.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class DebuggerTest {
    @Nullable
    private static String originalDebuggerSyspropValue;

    @BeforeAll
    static void beforeAll() {
        originalDebuggerSyspropValue = System.getProperty(Debugger.DEBUGGER_SYSPROP_NAME);
        System.clearProperty(Debugger.DEBUGGER_SYSPROP_NAME);
    }

    @AfterAll
    static void afterAll() {
        if (originalDebuggerSyspropValue != null) {
            System.setProperty(Debugger.DEBUGGER_SYSPROP_NAME, originalDebuggerSyspropValue);
        }
    }

    @BeforeEach
    void setUp() {
        assertSame(ReportingMode.OFF, Debugger.reportingMode());
    }

    @Test
    void reportingMode() {
        System.setProperty(Debugger.DEBUGGER_SYSPROP_NAME, ReportingMode.LOG_AND_THROW.toString());
        try {
            assertSame(ReportingMode.LOG_AND_THROW, Debugger.reportingMode());
        } finally {
            System.clearProperty(Debugger.DEBUGGER_SYSPROP_NAME);
            assertSame(ReportingMode.OFF, Debugger.reportingMode());
        }
    }

    @Test
    @SuppressWarnings("try")
    void useReportingMode() {
        System.setProperty(Debugger.DEBUGGER_SYSPROP_NAME, ReportingMode.LOG_AND_THROW.toString());
        try {
            try (OverridingReportingMode ignore = Debugger.useReportingMode(ReportingMode.OFF)) {
                assertSame(ReportingMode.OFF, Debugger.reportingMode());
            }
            assertSame(ReportingMode.LOG_AND_THROW, Debugger.reportingMode());
        } finally {
            System.clearProperty(Debugger.DEBUGGER_SYSPROP_NAME);
            assertSame(ReportingMode.OFF, Debugger.reportingMode());
        }
    }

    @Test
    @SuppressWarnings("try")
    void useReportingModeConcurrently() {
        try (OverridingReportingMode ignore = Debugger.useReportingMode(ReportingMode.LOG)) {
            assertAll(
                    () -> assertThrows(AssertionError.class, () -> Debugger.useReportingMode(ReportingMode.LOG)),
                    () -> assertThrows(AssertionError.class, () -> Debugger.useReportingMode(ReportingMode.LOG_AND_THROW))
            );
        }
        assertSame(ReportingMode.OFF, Debugger.reportingMode());
    }

    @Test
    void toStringThrowable() {
        assertAll(
                () -> {
                    Throwable t = new MongoDebuggingException("testMessage");
                    t.setStackTrace(new StackTraceElement[0]);
                    String tStr = Debugger.toString(t);
                    assertTrue(tStr.contains(t.getClass().getSimpleName()));
                    assertTrue(tStr.contains(t.getMessage()));
                },
                () -> {
                    Throwable t = new MongoDebuggingException("testMessage");
                    t.setStackTrace(new StackTraceElement[] {
                            new StackTraceElement("class1", "method1", "fileName1", 123),
                            new StackTraceElement("class2", "method2", "fileName2", 321)
                    });
                    String tStr = Debugger.toString(t);
                    assertTrue(tStr.contains(t.getClass().getSimpleName()));
                    assertTrue(tStr.contains(t.getMessage()));
                    assertTrue(tStr.contains(t.getStackTrace()[0].toString()));
                    assertTrue(tStr.contains(t.getStackTrace()[1].toString()));
                    assertTrue(tStr.contains(Debugger.FAKE_LINE_SEPARATOR));
                    assertFalse(Pattern.matches("\\v", tStr));
                }
        );
    }
}
