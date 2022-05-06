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
import com.mongodb.internal.client.model.AbstractConstructibleBson;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;

import static com.mongodb.assertions.Assertions.notNull;

final class SearchConstructibleBson extends AbstractConstructibleBson<SearchConstructibleBson> implements
        SearchOperator,
        SearchScore,
        RelevanceSearchScoreExpression, ConstantSearchScoreExpression, LogSearchScoreExpression, Log1pSearchScoreExpression,
        SearchCollector,
        SearchOptions,
        SearchHighlight,
        TotalSearchCount, LowerBoundSearchCount,
        SearchFuzzy,
        FieldSearchPath, WildcardSearchPath,
        SearchFacet {
    /**
     * An {@linkplain Immutable immutable} empty instance.
     */
    static final SearchConstructibleBson EMPTY = new SearchConstructibleBson(new BsonDocument());

    SearchConstructibleBson(final Bson base) {
        super(base);
    }

    private SearchConstructibleBson(final Bson base, final Document appended) {
        super(base, appended);
    }

    @Override
    protected SearchConstructibleBson newSelf(final Bson base, final Document appended) {
        return new SearchConstructibleBson(base, appended);
    }

    @Override
    public SearchOptions index(final String name) {
        return newAppended("index", new BsonString(notNull("name", name)));
    }

    @Override
    public SearchOptions highlight(final SearchHighlight option) {
        return newAppended("highlight", notNull("option", option));
    }

    @Override
    public SearchOptions count(final SearchCount option) {
        return newAppended("count", notNull("option", option));
    }

    @Override
    public SearchOptions returnStoredSource(final boolean returnStoredSource) {
        return newAppended("returnStoredSource", new BsonBoolean(returnStoredSource));
    }

    @Override
    public SearchOptions option(final String name, final Object value) {
        return newAppended(notNull("name", name), notNull("value", value));
    }

    @Override
    public SearchHighlight maxCharsToExamine(final int maxCharsToExamine) {
        return newAppended("maxCharsToExamine", new BsonInt32(maxCharsToExamine));
    }

    @Override
    public SearchHighlight maxNumPassages(final int maxNumPassages) {
        return newAppended("maxNumPassages", new BsonInt32(maxNumPassages));
    }

    @Override
    public LowerBoundSearchCount threshold(final int threshold) {
        return newAppended("threshold", new BsonInt32(threshold));
    }

    @Override
    public SearchFuzzy maxEdits(final int maxEdits) {
        return newAppended("maxEdits", maxEdits);
    }

    @Override
    public SearchFuzzy prefixLength(final int prefixLength) {
        return newAppended("prefixLength", prefixLength);
    }

    @Override
    public SearchFuzzy maxExpansions(final int maxExpansions) {
        return newAppended("maxExpansions", maxExpansions);
    }

    @Override
    public FieldSearchPath multi(final String analyzerName) {
        return newAppended("multi", new BsonString(notNull("analyzerName", analyzerName)));
    }

    @Override
    public SearchOperator score(final SearchScore modifier) {
        return newAppended("score", notNull("modifier", modifier));
    }
}
