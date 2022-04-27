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

import static org.bson.assertions.Assertions.notNull;

final class SearchConstructibleBsonElement extends AbstractConstructibleBsonElement<SearchConstructibleBsonElement> implements
        CompoundSearchOperator, ExistsSearchOperator, TextSearchOperator,
        FacetSearchCollector,
        StringSearchFacet, NumericSearchFacet, DateSearchFacet {
    SearchConstructibleBsonElement(final String name, final Bson value) {
        super(name, value);
    }

    @Override
    protected SearchConstructibleBsonElement newSelf(final String name, final Bson value) {
        return new SearchConstructibleBsonElement(name, value);
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
    public TextSearchOperator fuzzy(final FuzzySearchOptions options) {
        return newWithMutatedValue(doc -> {
            doc.remove("synonyms");
            doc.append("fuzzy", notNull("options", options));
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
    public CompoundSearchOperator minimumShouldMatch(final int minimumShouldMatch) {
        return newWithAppendedValue("minimumShouldMatch", new BsonInt32(minimumShouldMatch));
    }
}
