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

package com.mongodb.test;

import com.mongodb.test.extension.FlakyTestExtension;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * {@code @FlakyTest} is used to signal that the annotated method contains a flaky / racy test.
 *
 * <p>The test will be repeated up to a {@linkplain #maxAttempts maximum number of times} with a
 * configurable {@linkplain #name display name}. Each invocation will be repeated if the previous test fails.
 */
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Execution(SAME_THREAD) // cannot be run in parallel
@ExtendWith(FlakyTestExtension.class)
@TestTemplate
public @interface FlakyTest {

    /**
     * Placeholder for the {@linkplain TestInfo#getDisplayName display name} of
     * a {@code @RepeatedTest} method: <code>{displayName}</code>
     */
    String DISPLAY_NAME_PLACEHOLDER = "{displayName}";

    /**
     * Placeholder for the current repetition count of a {@code @FlakyTest}
     * method: <code>{index}</code>
     */
    String CURRENT_REPETITION_PLACEHOLDER = "{index}";

    /**
     * Placeholder for the total number of repetitions of a {@code @FlakyTest}
     * method: <code>{totalRepetitions}</code>
     */
    String TOTAL_REPETITIONS_PLACEHOLDER = "{totalRepetitions}";

    /**
     * <em>Short</em> display name pattern for a repeated test: {@value #SHORT_DISPLAY_NAME}
     *
     * @see #CURRENT_REPETITION_PLACEHOLDER
     * @see #TOTAL_REPETITIONS_PLACEHOLDER
     * @see #LONG_DISPLAY_NAME
     */
    String SHORT_DISPLAY_NAME = "Attempt: " + CURRENT_REPETITION_PLACEHOLDER + " / " + TOTAL_REPETITIONS_PLACEHOLDER;

    /**
     * <em>Long</em> display name pattern for a repeated test: {@value #LONG_DISPLAY_NAME}
     *
     * @see #DISPLAY_NAME_PLACEHOLDER
     * @see #SHORT_DISPLAY_NAME
     */
    String LONG_DISPLAY_NAME = DISPLAY_NAME_PLACEHOLDER + " " + SHORT_DISPLAY_NAME;

    /**
     * max number of attempts
     *
     * @return N-times repeat test if it failed
     */
    int maxAttempts() default 1;

    /**
     * Display name for test method
     *
     * @return Short name
     */
    String name() default LONG_DISPLAY_NAME;
}
