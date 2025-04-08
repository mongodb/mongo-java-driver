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

import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertNotNull;

final class UnifiedTestFailureValidator extends UnifiedSyncTest {
    private Throwable exception;

    @Override
    @BeforeEach
    public void setUp(
            final String testName,
            @Nullable final String fileDescription,
            @Nullable final String testDescription,
            final String directoryName,
            final int attemptNumber,
            final int totalAttempts,
            final String schemaVersion,
            @Nullable final BsonArray runOnRequirements,
            final BsonArray entitiesArray,
            final BsonArray initialData,
            final BsonDocument definition) {
        try {
            super.setUp(
                    testName,
                    fileDescription,
                    testDescription,
                    directoryName,
                    attemptNumber,
                    totalAttempts,
                    schemaVersion,
                    runOnRequirements,
                    entitiesArray,
                    initialData,
                    definition);
        } catch (AssertionError | Exception e) {
            exception = e;
        }
    }

    @Override
    @ParameterizedTest
    @MethodSource("data")
    public void shouldPassAllOutcomes(
            final String testName,
            @Nullable final String fileDescription,
            @Nullable final String testDescription,
            @Nullable final String directoryName,
            final int attemptNumber,
            final int totalAttempts,
            final String schemaVersion,
            @Nullable final BsonArray runOnRequirements,
            final BsonArray entitiesArray,
            final BsonArray initialData,
            final BsonDocument definition) {
        if (exception == null) {
            try {
                super.shouldPassAllOutcomes(
                        testName,
                        fileDescription,
                        testDescription,
                        directoryName,
                        attemptNumber,
                        totalAttempts,
                        schemaVersion,
                        runOnRequirements,
                        entitiesArray,
                        initialData,
                        definition);
            } catch (AssertionError | Exception e) {
                exception = e;
            }
        }
        assertNotNull(exception, "Expected exception but not was thrown");
    }

    private static Collection<Arguments> data() {
        return getTestData("unified-test-format/tests/valid-fail");
    }
}
