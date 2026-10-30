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

package com.mongodb.client;

import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.TreeSet;

import static java.lang.String.format;
import static java.util.Arrays.stream;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toCollection;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class ListCollectionNamesIterableTest {
    public static final Set<String> EXPECTED_EXTRA_METHODS = singleton("authorizedCollections(boolean)");
    public static final Set<String> EXPECTED_MISSING_METHODS = singleton("timeoutMode(com.mongodb.client.cursor.TimeoutMode)");

    @Test
    void declaredMethodsMatchListCollectionsIterable() {
        assertDeclaredMethodsMatch(
                ListCollectionsIterable.class,
                EXPECTED_EXTRA_METHODS,
                EXPECTED_MISSING_METHODS,
                ListCollectionNamesIterable.class);
    }

    public static void assertDeclaredMethodsMatch(
            final Class<?> expectedClass,
            final Set<String> expectedExtraMethods,
            final Set<String> expectedMissingMethods,
            final Class<?> actualClass) {
        Set<String> expected = declaredMethodSignatures(expectedClass);
        for (String expectedExtraMethod : expectedExtraMethods) {
            assertFalse(expected.contains(expectedExtraMethod),
                    () -> format("%s declares %s, remove it from expected extra methods", expectedClass, expectedExtraMethod));
        }
        for (String expectedMissingMethod : expectedMissingMethods) {
            assertTrue(expected.contains(expectedMissingMethod),
                    () -> format("%s does not declare %s, remove it from expected missing methods", expectedClass, expectedMissingMethod));
        }
        expected.addAll(expectedExtraMethods);
        expected.removeAll(expectedMissingMethods);
        assertEquals(expected, declaredMethodSignatures(actualClass));
    }

    private static Set<String> declaredMethodSignatures(final Class<?> klass) {
        return stream(klass.getDeclaredMethods())
                .map(method -> stream(method.getParameterTypes())
                        .map(Class::getTypeName)
                        .collect(joining(", ", method.getName() + "(", ")")))
                // `TreeSet` results in the alphabetic order of signatures in `toString`, improving assertion messages
                .collect(toCollection(TreeSet::new));
    }
}
