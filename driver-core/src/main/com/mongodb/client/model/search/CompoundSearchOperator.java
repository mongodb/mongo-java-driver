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

import com.mongodb.annotations.Immutable;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.List;

@Immutable
public final class CompoundSearchOperator extends SearchOperator {

    @Nullable
    private final List<SearchOperator> mustClause;
    @Nullable
    private final List<SearchOperator> mustNotClause;
    @Nullable
    private final List<SearchOperator> shouldClause;
    @Nullable
    private final List<SearchOperator> filterClause;
    @Nullable
    private final Integer minimumShouldMatch;

    CompoundSearchOperator(@Nullable List<SearchOperator> mustClause, @Nullable List<SearchOperator> mustNotClause,
            @Nullable List<SearchOperator> shouldClause, @Nullable List<SearchOperator> filterClause,
            @Nullable Integer minimumShouldMatch, @Nullable final String index) {
        super(index);
        this.mustClause = mustClause;
        this.mustNotClause = mustNotClause;
        this.shouldClause = shouldClause;
        this.filterClause = filterClause;
        this.minimumShouldMatch = minimumShouldMatch;
    }

    @Override
    public CompoundSearchOperator index(final String index) {
        return new CompoundSearchOperator(mustClause, mustNotClause, shouldClause, filterClause, minimumShouldMatch, index);
    }

    public CompoundSearchOperator minimumShouldMatch(final int value) {
        return new CompoundSearchOperator(mustClause, mustNotClause, shouldClause, filterClause, value, getIndex());
    }

    public CompoundSearchOperator must(final List<SearchOperator> mustClause) {
        return new CompoundSearchOperator(mustClause, mustNotClause, shouldClause, filterClause, minimumShouldMatch, getIndex());
    }

    public CompoundSearchOperator mustNot(final List<SearchOperator> mustNotClause) {
        return new CompoundSearchOperator(mustClause, mustNotClause, shouldClause, filterClause, minimumShouldMatch, getIndex());
    }

    public CompoundSearchOperator should(final List<SearchOperator> shouldClause) {
        return new CompoundSearchOperator(mustClause, mustNotClause, shouldClause, filterClause, minimumShouldMatch, getIndex());
    }

    public CompoundSearchOperator filter(final List<SearchOperator> filterClause) {
        return new CompoundSearchOperator(mustClause, mustNotClause, shouldClause, filterClause, minimumShouldMatch, getIndex());
    }

    @Override
    public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
        BsonDocument searchStageDocument = new BsonDocument();
        appendCommonFields(searchStageDocument);
        BsonDocument compoundOperatorDocument = new BsonDocument();

        appendClause("must", mustClause, compoundOperatorDocument, documentClass, codecRegistry);
        appendClause("mustNot", mustNotClause, compoundOperatorDocument, documentClass, codecRegistry);
        appendClause("should", shouldClause, compoundOperatorDocument, documentClass, codecRegistry);
        appendClause("filter", filterClause, compoundOperatorDocument, documentClass, codecRegistry);

        if (minimumShouldMatch != null) {
            compoundOperatorDocument.append("minimumShouldMatch", new BsonInt32(minimumShouldMatch));
        }

        searchStageDocument.append("compound", compoundOperatorDocument);

        return searchStageDocument;
    }

    private <TDocument> void appendClause(final String key, @Nullable final List<SearchOperator> clause,
            final BsonDocument compoundOperatorDocument, final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
        if (clause != null) {
            compoundOperatorDocument.append(key,
                    clause.stream().map((operator) -> operator.toBsonDocument(documentClass, codecRegistry))
                            .collect(BsonArray::new, BsonArray::add, BsonArray::addAll));
        }
    }
}
