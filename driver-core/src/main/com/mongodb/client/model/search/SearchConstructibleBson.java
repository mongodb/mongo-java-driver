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

import com.mongodb.internal.client.model.AbstractConstructibleBson;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.conversions.Bson;

import static com.mongodb.assertions.Assertions.notNull;

final class SearchConstructibleBson extends AbstractConstructibleBson<SearchConstructibleBson> implements
        SearchOperator,
        SearchCollector,
        SearchOptions,
        SearchHighlight,
        TotalSearchCount, LowerBoundSearchCount,
        FieldSearchPath, WildcardSearchPath,
        SearchFacet {
    static final SearchConstructibleBson EMPTY = new SearchConstructibleBson(new BsonDocument());

    SearchConstructibleBson(final Bson doc) {
        super(doc);
    }

    @Override
    protected SearchConstructibleBson newSelf(final BsonDocument doc) {
        return new SearchConstructibleBson(doc);
    }

    @Override
    public SearchOptions index(final String name) {
        return newAppended("index", new BsonString(notNull("name", name)));
    }

    @Override
    public SearchOptions highlight(final SearchHighlight option) {
        return newAppended("highlight", notNull("option", option).toBsonDocument());
    }

    @Override
    public SearchOptions count(final SearchCount option) {
        return newAppended("count", notNull("option", option).toBsonDocument());
    }

    @Override
    public SearchOptions returnStoredSource(final boolean returnStoredSource) {
        return newAppended("returnStoredSource", new BsonBoolean(returnStoredSource));
    }

    @Override
    public SearchOptions option(final String name, final BsonValue value) {
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
    public FieldSearchPath multi(final String analyzerName) {
        return newAppended("multi", new BsonString(notNull("analyzerName", analyzerName)));
    }
}
