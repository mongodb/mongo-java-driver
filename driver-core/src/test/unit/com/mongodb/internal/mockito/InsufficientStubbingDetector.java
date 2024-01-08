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
package com.mongodb.internal.mockito;

import com.mongodb.lang.Nullable;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.function.Consumer;

import static com.mongodb.assertions.Assertions.fail;
import static java.lang.String.format;

/**
 * @see MongoMockito#mock(Class, Consumer)
 */
final class InsufficientStubbingDetector implements Answer<Void> {
    private boolean enabled;

    InsufficientStubbingDetector() {
    }

    @Nullable
    @Override
    public Void answer(final InvocationOnMock invocation) throws AssertionError {
        if (enabled) {
            throw fail(format("Insufficient stubbing. Unexpected invocation %s on the object %s.", invocation, invocation.getMock()));
        }
        return null;
    }

    void enable() {
        enabled = true;
    }
}
