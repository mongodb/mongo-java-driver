/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.unified;

import com.mongodb.internal.logging.StructuredLogMessage;
import com.mongodb.internal.logging.StructuredLogger;
import com.mongodb.internal.logging.StructuredLoggingInterceptor;
import com.mongodb.lang.NonNull;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class TestLoggingInterceptor implements StructuredLoggingInterceptor, AutoCloseable {

    private final List<StructuredLogMessage> messages = new ArrayList<>();
    private final String applicationName;

    public TestLoggingInterceptor(final String applicationName) {
        this.applicationName = requireNonNull(applicationName);
        StructuredLogger.addInterceptor(applicationName, this);
    }

    @Override
    public synchronized void intercept(@NonNull final StructuredLogMessage message) {
        messages.add(message);
    }

    @Override
    public void close() {
        StructuredLogger.removeInterceptor(applicationName);
    }

    public synchronized List<StructuredLogMessage> getMessages() {
        return new ArrayList<>(messages);
    }
}
