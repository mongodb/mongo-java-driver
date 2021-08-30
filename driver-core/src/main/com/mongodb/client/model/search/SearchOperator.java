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
import org.bson.BsonString;
import org.bson.conversions.Bson;

import java.util.List;

/**
 *
 * @since 4.4
 */
@Immutable
public abstract class SearchOperator implements Bson {
    @Nullable
    private final String index;

    SearchOperator(@Nullable final String index) {
        this.index = index;
    }

    @Nullable
    public String getIndex() {
        return index;
    }

    abstract public SearchOperator index(final String index);

    void appendCommonFields(final BsonDocument searchStageDocument) {
        if (index != null) {
            searchStageDocument.append("index", new BsonString(index));
        }
    }

    void appendPath(final BsonDocument searchOperatorDocument, final List<SearchPath> path) {
        if (path.size() == 1) {
            searchOperatorDocument.append("path", path.get(0).toBsonValue());
        } else {
            searchOperatorDocument.append("path",
                    path.stream().map(SearchPath::toBsonValue).collect(BsonArray::new, BsonArray::add, BsonArray::addAll));
        }
    }

    void appendScore(final BsonDocument searchOperatorDocument, @Nullable final SearchScore score) {
        if (score != null) {
            searchOperatorDocument.append("score", score.toBsonValue());
        }
    }
}
