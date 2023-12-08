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
import org.mockito.Answers;
import org.mockito.Mockito;
import org.mockito.internal.stubbing.answers.ThrowsException;
import org.mockito.stubbing.OngoingStubbing;

import java.util.function.Consumer;

import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/**
 * Complements {@link Mockito}.
 */
public final class MongoMockito {
    /**
     * Is equivalent to calling {@link #mock(Class, Consumer)} with a {@code null} {@code tuner}.
     */
    public static <T> T mock(final Class<T> classToMock) {
        return mock(classToMock, null);
    }

    /**
     * This method is similar to {@link Mockito#mock(Class)} but changes the default behavior of the methods of a mock object
     * such that insufficient stubbing is detected and reported. By default, Mockito uses {@link Answers#RETURNS_DEFAULTS}.
     * While this answer has potential to save users some stubbing work, the provided convenience may not be worth the cost:
     * if the default result (often {@code null} for reference types) is insufficient,
     * one likely gets an unhelpful {@link NullPointerException}
     * (see {@link InsufficientStubbingDetectorDemoTest#mockObjectWithDefaultAnswer()}),
     * or a silent incorrect behavior with no clear indication of the mock object method that caused the problem.
     * Furthermore, a working test that uses mock objects may be unwittingly broken when refactoring production code.
     * While this particular issue is inherent to tests that use mock objects,
     * broken tests not indicating clearly what is wrong make matters worse.
     * <p>
     * Mockito has {@link ThrowsException},
     * and at first glance it may seem like using it may help detecting insufficient stubbing.
     * It can point us to a line where the insufficiently stubbed method was called at, but it cannot tell us the name of that method
     * (see {@link InsufficientStubbingDetectorDemoTest#mockObjectWithThrowsException()}).
     * Moreover, a mock object created with {@link ThrowsException} as its default answer cannot be stubbed:
     * stubbing requires calling methods of the mock object, but they all complete abruptly
     * (see {@link InsufficientStubbingDetectorDemoTest#stubbingWithThrowsException()}).
     * Therefore, {@link ThrowsException} is not suitable for detecting insufficient stubbing.</p>
     * <p>
     * This method overcomes both of the aforementioned limitations by using {@link InsufficientStubbingDetector} as the default answer
     * (see {@link InsufficientStubbingDetectorDemoTest#mockObjectWithInsufficientStubbingDetector()},
     * {@link InsufficientStubbingDetectorDemoTest#stubbingWithInsufficientStubbingDetector()}).
     * Note also that for convenience, {@link InsufficientStubbingDetector} stubs the {@link Object#toString()} method by using
     * {@link OngoingStubbing#thenCallRealMethod()}, unless this stubbing is overwritten by the {@code tuner}.</p>
     */
    public static <T> T mock(final Class<T> classToMock, @Nullable final Consumer<T> tuner) {
        final InsufficientStubbingDetector insufficientStubbingDetector = new InsufficientStubbingDetector();
        final T mock = Mockito.mock(classToMock, withSettings().defaultAnswer(insufficientStubbingDetector));
        when(mock.toString()).thenCallRealMethod();
        if (tuner != null) {
            tuner.accept(mock);
        }
        insufficientStubbingDetector.enable();
        return mock;
    }

    private MongoMockito() {
    }
}
