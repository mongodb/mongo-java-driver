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
package com.mongodb.client.model.search;

import com.mongodb.Function;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static com.mongodb.client.model.search.SearchOperator.exists;
import static com.mongodb.client.model.search.SearchPath.fieldPath;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

final class SearchOperatorCombinationTest {
    @ParameterizedTest(name = "{index} {0}")
    @MethodSource("args")
    void test(final String expectedRuleName, final Function<Iterable<? extends SearchOperator>, SearchOperatorCombination> combiner) {
        assertAll(
                () -> assertThrows(IllegalArgumentException.class, () ->
                        // clauses must not be empty
                        combiner.apply(emptyList())
                ),
                () -> assertEquals(
                        new BsonDocument(expectedRuleName, new BsonArray(singletonList(
                                exists(fieldPath("fieldName")).toBsonDocument()))
                        ),
                        combiner.apply(singletonList(
                                exists(fieldPath("fieldName"))))
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument(expectedRuleName, new BsonArray(asList(
                                exists(fieldPath("fieldName1")).toBsonDocument(),
                                exists(fieldPath("fieldName2")).toBsonDocument()))
                        ),
                        combiner.apply(asList(
                                exists(fieldPath("fieldName1")),
                                exists(fieldPath("fieldName2"))))
                                .toBsonDocument()
                )
        );
    }

    /**
     * @see #test(String, Function)
     */
    private static Stream<Arguments> args() {
        return Stream.of(
                arguments("must",
                        (Function<Iterable<? extends SearchOperator>, SearchOperatorCombination>) SearchOperatorCombination::must),
                arguments("mustNot",
                        (Function<Iterable<? extends SearchOperator>, SearchOperatorCombination>) SearchOperatorCombination::mustNot),
                arguments("should",
                        (Function<Iterable<? extends SearchOperator>, SearchOperatorCombination>) SearchOperatorCombination::should),
                arguments("filter",
                        (Function<Iterable<? extends SearchOperator>, SearchOperatorCombination>) SearchOperatorCombination::filter)
        );
    }
}
