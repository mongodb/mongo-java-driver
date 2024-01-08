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
package com.mongodb.test.extension;

import com.mongodb.test.FlakyTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler;
import org.junit.jupiter.api.extension.TestInstantiationException;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.platform.commons.util.Preconditions;
import org.opentest4j.TestAbortedException;

import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.stream.Stream;

import static com.mongodb.test.FlakyTest.CURRENT_REPETITION_PLACEHOLDER;
import static com.mongodb.test.FlakyTest.DISPLAY_NAME_PLACEHOLDER;
import static com.mongodb.test.FlakyTest.TOTAL_REPETITIONS_PLACEHOLDER;
import static java.util.Collections.singletonList;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;
import static org.junit.platform.commons.util.AnnotationUtils.findAnnotation;
import static org.junit.platform.commons.util.AnnotationUtils.isAnnotated;


/**
 * A {@code TestTemplateInvocationContextProvider} that supports the {@link FlakyTest @FlakyTest} annotation.
 */
public class FlakyTestExtension implements TestTemplateInvocationContextProvider,
        BeforeTestExecutionCallback,
        AfterTestExecutionCallback,
        TestExecutionExceptionHandler {

    private int maxAttempts = 0;
    private FlakyTestDisplayFormatter formatter;
    private Boolean testHasPassed;
    private int currentAttempt = 0;


    @Override
    public void afterTestExecution(final ExtensionContext extensionContext) {
        testHasPassed = extensionContext.getExecutionException().map(e -> e instanceof TestInstantiationException).orElse(true);
    }

    @Override
    public boolean supportsTestTemplate(final ExtensionContext context) {
        return isAnnotated(context.getTestMethod(), FlakyTest.class);
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(final ExtensionContext context) {
        Method testMethod = context.getRequiredTestMethod();
        String displayName = context.getDisplayName();

        if (isAnnotated(testMethod, Test.class)) {
            throw new TestInstantiationException(String.format("Test %s also annotated with @Test", displayName));
        } else if (isAnnotated(testMethod, ParameterizedTest.class)) {
            throw new TestInstantiationException(String.format("Test %s also annotated with @ParameterizedTest", displayName));
        }

        FlakyTest flakyTest = findAnnotation(testMethod, FlakyTest.class)
                .orElseThrow(() ->
                        new TestInstantiationException("The extension should not be executed unless the test method is "
                                + "annotated with @FlakyTest."));

        formatter = displayNameFormatter(flakyTest, testMethod, displayName);

        maxAttempts = flakyTest.maxAttempts();
        Preconditions.condition(maxAttempts > 0, "Total repeats must be higher than 0");

        //Convert logic of repeated handler to spliterator
        Spliterator<TestTemplateInvocationContext> spliterator =
                spliteratorUnknownSize(new TestTemplateIterator(), Spliterator.NONNULL);
        return stream(spliterator, false);
    }

    private FlakyTestDisplayFormatter displayNameFormatter(final FlakyTest flakyTest, final Method method,
            final String displayName) {
        String pattern = Preconditions.notBlank(flakyTest.name().trim(), () -> String.format(
                "Configuration error: @FlakyTest on method [%s] must be declared with a non-empty name.", method));
        return new FlakyTestDisplayFormatter(pattern, displayName);
    }

    @Override
    public void handleTestExecutionException(final ExtensionContext context, final Throwable throwable) throws Throwable {
        if (currentAttempt < maxAttempts) {
            // Mark failure as skipped / aborted so to pass CI
            throw new TestAbortedException("Test failed on attempt: " + currentAttempt);
        }
        throw throwable;
    }

    @Override
    public void beforeTestExecution(final ExtensionContext context) {
        currentAttempt++;
    }

    /**
     * TestTemplateIterator (Repeat test if it failed)
     */
    class TestTemplateIterator implements Iterator<TestTemplateInvocationContext> {
        private int currentIndex = 0;

        @Override
        public boolean hasNext() {
            if (currentIndex == 0) {
                return true;
            }
            return !testHasPassed && currentIndex < maxAttempts;
        }

        @Override
        public TestTemplateInvocationContext next() {
            if (hasNext()) {
                currentIndex++;
                return new RepeatInvocationContext(currentIndex, maxAttempts, formatter);
            }
            throw new NoSuchElementException();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    static class RepeatInvocationContext implements TestTemplateInvocationContext {
        private final int currentRepetition;
        private final int totalTestRuns;
        private final FlakyTestDisplayFormatter formatter;

        RepeatInvocationContext(final int currentRepetition, final int totalRepetitions, final FlakyTestDisplayFormatter formatter) {
            this.currentRepetition = currentRepetition;
            this.totalTestRuns = totalRepetitions;
            this.formatter = formatter;
        }

        @Override
        public String getDisplayName(final int invocationIndex) {
            return formatter.format(currentRepetition, totalTestRuns);
        }

        @Override
        public List<Extension> getAdditionalExtensions() {
            return singletonList((ExecutionCondition) context -> {
                if (currentRepetition > totalTestRuns) {
                    return ConditionEvaluationResult.disabled("All attempts failed");
                } else {
                    return ConditionEvaluationResult.enabled("Test failed - retry");
                }
            });
        }
    }

    static class FlakyTestDisplayFormatter {
        private final String pattern;
        private final String displayName;

        FlakyTestDisplayFormatter(final String pattern, final String displayName) {
            this.pattern = pattern;
            this.displayName = displayName;
        }

        String format(final int currentRepetition, final int totalRepetitions) {
            return pattern
                    .replace(DISPLAY_NAME_PLACEHOLDER, displayName)
                    .replace(CURRENT_REPETITION_PLACEHOLDER, String.valueOf(currentRepetition))
                    .replace(TOTAL_REPETITIONS_PLACEHOLDER, String.valueOf(totalRepetitions));
        }

    }

}
