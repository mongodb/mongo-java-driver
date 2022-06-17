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

import com.mongodb.internal.client.model.AbstractConstructibleBsonElement;
import org.bson.BsonInt32;
import org.bson.conversions.Bson;

import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.client.model.Util.sizeAtLeast;

final class SearchConstructibleBsonElement extends AbstractConstructibleBsonElement<SearchConstructibleBsonElement> implements
        CompoundSearchOperatorBase, CompoundSearchOperator,
        MustCompoundSearchOperator, MustNotCompoundSearchOperator, ShouldCompoundSearchOperator, FilterCompoundSearchOperator,
        ExistsSearchOperator, TextSearchOperator, AutocompleteSearchOperator,
        NumberNearSearchOperator, DateNearSearchOperator, GeoNearSearchOperator,
        ValueBoostSearchScore, PathBoostSearchScore, ConstantSearchScore, FunctionSearchScore,
        GaussSearchScoreExpression, PathSearchScoreExpression,
        FacetSearchCollector,
        StringSearchFacet, NumberSearchFacet, DateSearchFacet {
    SearchConstructibleBsonElement(final String name) {
        super(name);
    }

    SearchConstructibleBsonElement(final String name, final Bson value) {
        super(name, value);
    }

    SearchConstructibleBsonElement(final Bson baseElement) {
        super(baseElement);
    }

    private SearchConstructibleBsonElement(final Bson baseElement, final Bson appendedElementValue) {
        super(baseElement, appendedElementValue);
    }

    @Override
    protected SearchConstructibleBsonElement newSelf(final Bson baseElement, final Bson appendedElementValue) {
        return new SearchConstructibleBsonElement(baseElement, appendedElementValue);
    }

    @Override
    public StringSearchFacet numBuckets(final int max) {
        return newWithAppendedValue("numBuckets", max);
    }

    @Override
    public SearchConstructibleBsonElement defaultBucket(final String name) {
        return newWithAppendedValue("default", notNull("name", name));
    }

    @Override
    public SearchConstructibleBsonElement fuzzy(final SearchFuzzy option) {
        return newWithMutatedValue(doc -> {
            doc.remove("synonyms");
            doc.append("fuzzy", notNull("option", option));
        });
    }

    @Override
    public TextSearchOperator synonyms(final String name) {
        return newWithMutatedValue(doc -> {
            doc.remove("fuzzy");
            doc.append("synonyms", notNull("name", name));
        });
    }

    @Override
    public AutocompleteSearchOperator anyTokenOrder() {
        return newWithAppendedValue("tokenOrder", "any");
    }

    @Override
    public AutocompleteSearchOperator sequentialTokenOrder() {
        return newWithAppendedValue("tokenOrder", "sequential");
    }

    @Override
    public MustCompoundSearchOperator must(final Iterable<? extends SearchOperator> clauses) {
        return newCombined("must", clauses);
    }

    @Override
    public MustNotCompoundSearchOperator mustNot(final Iterable<? extends SearchOperator> clauses) {
        return newCombined("mustNot", clauses);
    }

    @Override
    public ShouldCompoundSearchOperator should(final Iterable<? extends SearchOperator> clauses) {
        return newCombined("should", clauses);
    }

    @Override
    public FilterCompoundSearchOperator filter(final Iterable<? extends SearchOperator> clauses) {
        return newCombined("filter", clauses);
    }

    private SearchConstructibleBsonElement newCombined(final String ruleName, final Iterable<? extends SearchOperator> clauses) {
        notNull("clauses", clauses);
        isTrueArgument("clauses must not be empty", sizeAtLeast(clauses, 1));
        return newWithMutatedValue(doc -> {
            Iterable<?> existingClauses = doc.get(ruleName, Iterable.class);
            final Iterable<?> newClauses;
            if (existingClauses == null) {
                newClauses = clauses;
            } else {
                newClauses = Stream.concat(
                        StreamSupport.stream(existingClauses.spliterator(), false),
                        StreamSupport.stream(clauses.spliterator(), false)).collect(Collectors.toList());
            }
            doc.append(ruleName, newClauses);
        });
    }

    @Override
    public ShouldCompoundSearchOperator minimumShouldMatch(final int minimumShouldMatch) {
        return newWithAppendedValue("minimumShouldMatch", new BsonInt32(minimumShouldMatch));
    }

    @Override
    public SearchConstructibleBsonElement score(final SearchScore modifier) {
        return newWithAppendedValue("score", notNull("modifier", modifier));
    }

    @Override
    public SearchConstructibleBsonElement undefined(final float fallback) {
        return newWithAppendedValue("undefined", fallback);
    }

    @Override
    public GaussSearchScoreExpression offset(final double offset) {
        return newWithAppendedValue("offset", offset);
    }

    @Override
    public GaussSearchScoreExpression decay(final double decay) {
        return newWithAppendedValue("decay", decay);
    }
}
