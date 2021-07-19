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

import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SocketStreamFactory;
import com.mongodb.connection.SslSettings;
import com.mongodb.connection.StreamFactory;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

final class InternalConnectionDebuggerTest {
    @Test
    void debuggableStreamFactory() {
        StreamFactory streamFactory = new SocketStreamFactory(SocketSettings.builder().build(), SslSettings.builder().build());
        assertAll(
                () -> assertSame(streamFactory, new InternalConnectionDebugger(Debugger.ReportingMode.OFF)
                        .debuggableStreamFactory(streamFactory)),
                () -> assertNotSame(streamFactory, new InternalConnectionDebugger(Debugger.ReportingMode.LOG)
                        .debuggableStreamFactory(streamFactory)),
                () -> assertNotSame(streamFactory, new InternalConnectionDebugger(Debugger.ReportingMode.LOG_AND_THROW)
                        .debuggableStreamFactory(streamFactory))
        );
    }

    @Test
    void report() {
        MongoDebuggingException e = new MongoDebuggingException();
        Reporter.FailureCallback mustNotBeCalled = t -> fail();
        assertAll(
                () -> assertFalse(new InternalConnectionDebugger(Debugger.ReportingMode.OFF)
                        .report(e, null)),
                () -> assertFalse(new InternalConnectionDebugger(Debugger.ReportingMode.OFF)
                        .report(e, mustNotBeCalled)),
                () -> assertFalse(new InternalConnectionDebugger(Debugger.ReportingMode.LOG)
                        .report(e, null)),
                () -> assertFalse(new InternalConnectionDebugger(Debugger.ReportingMode.LOG)
                        .report(e, mustNotBeCalled)),
                () -> assertThrows(e.getClass(), () -> new InternalConnectionDebugger(Debugger.ReportingMode.LOG_AND_THROW)
                        .report(e, null)),
                () -> {
                    Reporter.FailureCallback callback = mock(Reporter.FailureCallback.class);
                    assertTrue(new InternalConnectionDebugger(Debugger.ReportingMode.LOG_AND_THROW)
                            .report(e, callback));
                    ArgumentCaptor<MongoDebuggingException> callbackArgCaptor = ArgumentCaptor.forClass(MongoDebuggingException.class);
                    verify(callback).execute(callbackArgCaptor.capture());
                    assertSame(e, callbackArgCaptor.getValue().getCause());
                }
        );
    }

    @Test
    void dataCollector() {
        assertAll(
                () -> assertFalse(new InternalConnectionDebugger(Debugger.ReportingMode.OFF)
                        .dataCollector().isPresent()),
                () -> assertTrue(new InternalConnectionDebugger(Debugger.ReportingMode.LOG)
                        .dataCollector().isPresent()),
                () -> assertTrue(new InternalConnectionDebugger(Debugger.ReportingMode.LOG_AND_THROW)
                        .dataCollector().isPresent())
        );
    }
}
