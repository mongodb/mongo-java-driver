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
import org.bson.codecs.configuration.CodecRegistry;

import java.util.Collections;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * Builder for the {@code text} operator for Atlas search.
 *
 * <p>
 * The text operator performs a full-text search using the analyzer specified in the index configuration. If you do not specify an
 * analyzer, the default standard analyzer is used.      
 * </p>
 *
 * @see SearchOperators#text(String, SearchPath)
 * @see SearchOperators#text(List, List)
 * @mongodb.driver.manual.atlas reference/atlas-search/text/  
 * @since 4.4
 */
@Immutable
public final class TextSearchOperator extends SearchOperator {

    private final List<String> query;
    private final List<SearchPath> path;
    @Nullable
    private final SearchScore score;

    TextSearchOperator(final List<String> query, final List<SearchPath> path, @Nullable SearchScore score, @Nullable final String index) {
        super(index);
        this.query = notNull("query", query);
        this.path = notNull("path", path);
        this.score = score;
    }

    /**
     * The string or strings to search for. If there are multiple terms in a string, Atlas Search also looks for a match for each term in
     * the string separately.
     * 
     * @return the query
     */
    public List<String> getQuery() {
        return Collections.unmodifiableList(query);
    }

    /**
     * The indexed field or fields to search.
     *
     * @return the path
     */
    public List<SearchPath> getPath() {
        return Collections.unmodifiableList(path);
    }

    /**
     * The score assigned to matching search term results.
     *
     * @return the score
     */
    @Nullable
    public SearchScore getScore() {
        return score;
    }

    /**
     * Set the score assigned to matching search term results.
     *
     * @param score the score
     * @return a new instance of {@code TextSearchOperator} which is a copy of {@code this}, but using the given score.
     */
    public TextSearchOperator score(final SearchScore score) {
        return new TextSearchOperator(query, path, score, getIndex());
    }

    /**
     * Set the index to use for the search.
     *
     * @param index the index
     * @return a new instance of {@code TextSearchOperator} which is a copy of {@code this}, but using the given index.
     */
    @Override
    public TextSearchOperator index(final String index) {
        return new TextSearchOperator(query, path, score, index);
    }

    @Override
    public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
        BsonDocument searchStageDocument = new BsonDocument();
        appendCommonFields(searchStageDocument);
        BsonDocument textOperatorDocument = new BsonDocument();
        if (query.size() == 1) {
            textOperatorDocument.append("query", new BsonString(query.get(0)));
        } else {
            textOperatorDocument.append("query", query.stream().map(BsonString::new).collect(BsonArray::new, BsonArray::add, BsonArray::addAll));
        }

        appendPath(textOperatorDocument, path);

        appendScore(textOperatorDocument, score);

        searchStageDocument.append("text", textOperatorDocument);

        return searchStageDocument;
    }
}
