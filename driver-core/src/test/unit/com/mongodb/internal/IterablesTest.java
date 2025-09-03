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
package com.mongodb.internal;

import org.junit.jupiter.api.Test;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class IterablesTest {
    @Test
    void concat() {
        assertAll(
                () -> assertIterable(singletonList(null), Iterables.concat(null)),
                () -> assertIterable(singletonList(null), Iterables.concat(null, (Object[]) null)),
                () -> assertIterable(singletonList(1), Iterables.concat(1)),
                () -> assertIterable(singletonList(1), Iterables.concat(1, (Object[]) null)),
                () -> assertIterable(asList(null, null), Iterables.concat(null, new Object[] {null})),
                () -> assertIterable(asList(null, null), Iterables.concat(null, singleton(null))),
                () -> assertIterable(asList(1, null), Iterables.concat(1, new Object[] {null})),
                () -> assertIterable(asList(1, null), Iterables.concat(1, singleton(null))),
                () -> assertIterable(asList(null, 1), Iterables.concat(null, 1)),
                () -> assertIterable(asList(null, 1), Iterables.concat(null, singleton(1))),
                () -> assertIterable(asList(1, 2), Iterables.concat(1, 2)),
                () -> assertIterable(asList(1, 2), Iterables.concat(1, singleton(2))),
                () -> assertIterable(asList(1, 2, 3), Iterables.concat(1, 2, 3)),
                () -> assertIterable(asList(1, 2, 3), Iterables.concat(1, asList(2, 3)))
        );
    }

    private static <T> void assertIterable(final List<? extends T> expected, final Iterable<? extends T> actual) {
        assertEquals(expected, stream(actual.spliterator(), false).collect(toList()));
        assertEquals(expected.toString(), actual.toString());
    }
}
