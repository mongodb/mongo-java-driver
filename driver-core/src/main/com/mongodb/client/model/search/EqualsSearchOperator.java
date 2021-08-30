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
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;

@Immutable
public final class EqualsSearchOperator extends SearchOperator {
    private final BsonValue value;
    private final List<SearchPath> path;

    EqualsSearchOperator(final BsonValue value, final List<SearchPath> path, @Nullable final String index) {
        super(index);
        this.value = notNull("value", value);
        this.path = notNull("path", path);
    }

    public BsonValue getValue() {
        return value;
    }

    public List<SearchPath> getPath() {
        return path;
    }

    @Override
    public EqualsSearchOperator index(final String index) {
        return new EqualsSearchOperator(value, path, index);
    }

    @Override
    public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
        BsonDocument searchStageDocument = new BsonDocument();
        appendCommonFields(searchStageDocument);
        BsonDocument equalsOperatorDocument = new BsonDocument("value", value);

        appendPath(equalsOperatorDocument, path);

        searchStageDocument.append("equals", equalsOperatorDocument);

        return searchStageDocument;
    }

    void appendPath(final BsonDocument searchOperatorDocument, final List<SearchPath> path) {
        if (path.size() == 1) {
            searchOperatorDocument.append("path", path.get(0).toBsonValue());
        } else {
            searchOperatorDocument.append("path", path.stream().map(SearchPath::toBsonValue).collect(BsonArray::new, BsonArray::add, BsonArray::addAll));
        }
    }
}
