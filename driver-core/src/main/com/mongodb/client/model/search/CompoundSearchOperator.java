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
import java.util.Map;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.Collections.unmodifiableMap;

@Immutable
public final class CompoundSearchOperator extends SearchOperator {

    private final Map<Clause, List<SearchOperator>> clauses;
    @Nullable
    private final Integer minimumShouldMatch;


    public enum Clause {
        MUST("must"),

        MUST_NOT("mustNot"),

        SHOULD("should"),

        FILTER("filter");

        private final String key;

        Clause(final String key) {
            this.key = key;
        }
    }

    CompoundSearchOperator(Map<Clause, List<SearchOperator>> clauses, @Nullable Integer minimumShouldMatch, @Nullable final String index) {
        super(index);
        this.clauses = unmodifiableMap(notNull("clauses", clauses));
        this.minimumShouldMatch = minimumShouldMatch;
    }

    @Override
    public CompoundSearchOperator index(final String index) {
        return new CompoundSearchOperator(clauses, minimumShouldMatch, index);
    }

    public CompoundSearchOperator minimumShouldMatch(final int value) {
        return new CompoundSearchOperator(clauses, value, getIndex());
    }

    @Override
    public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
        BsonDocument searchStageDocument = new BsonDocument();
        appendCommonFields(searchStageDocument);
        BsonDocument compoundOperatorDocument = new BsonDocument();

        clauses.forEach((key, value) -> {
            compoundOperatorDocument.append(key.key,
                    value.stream().map((operator) -> operator.toBsonDocument(documentClass, codecRegistry))
                            .collect(BsonArray::new, BsonArray::add, BsonArray::addAll));
        });

        if (minimumShouldMatch != null) {
            compoundOperatorDocument.append("minimumShouldMatch", new BsonInt32(minimumShouldMatch));
        }

        searchStageDocument.append("compound", compoundOperatorDocument);

        return searchStageDocument;
    }
}
