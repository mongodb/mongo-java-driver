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

import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;

@Immutable
public final class AutoCompleteSearchOperator extends SearchOperator {

    public enum TokenOrder {
        ANY("any"),

        SEQUENTIAL("sequential");

        private final String key;

        TokenOrder(final String key) {
            this.key = key;
        }
    }

    private final List<String> query;
    private final List<SearchPath> path;
    private final TokenOrder tokenOrder;
    private final Fuzzy fuzzy;
    private final SearchScore score;

    AutoCompleteSearchOperator(final List<String> query, final List<SearchPath> path, @Nullable final TokenOrder tokenOrder,
            @Nullable final Fuzzy fuzzy, @Nullable final SearchScore score, @Nullable final String index) {
        super(index);
        this.query = notNull("query", query);
        this.path = notNull("path", path);
        this.tokenOrder = tokenOrder;
        this.fuzzy = fuzzy;
        this.score = score;
    }

    public List<String> getQuery() {
        return query;
    }

    public List<SearchPath> getPath() {
        return path;
    }

    @Nullable
    public TokenOrder getTokenOrder() {
        return tokenOrder;
    }

    @Nullable
    public Fuzzy getFuzzy() {
        return fuzzy;
    }

    @Nullable
    public SearchScore getScore() {
        return score;
    }

    public AutoCompleteSearchOperator tokenOrder(TokenOrder tokenOrder) {
        return new AutoCompleteSearchOperator(query, path, tokenOrder, fuzzy, score, getIndex());
    }

    public AutoCompleteSearchOperator fuzzy(Fuzzy fuzzy) {
        return new AutoCompleteSearchOperator(query, path, tokenOrder, fuzzy, score, getIndex());
    }

    public AutoCompleteSearchOperator score(SearchScore score) {
        return new AutoCompleteSearchOperator(query, path, tokenOrder, fuzzy, score, getIndex());
    }

    @Override
    public AutoCompleteSearchOperator index(final String index) {
        return new AutoCompleteSearchOperator(query, path, tokenOrder, fuzzy, score, index);
    }

    @Override
    public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
        BsonDocument searchStageDocument = new BsonDocument();
        appendCommonFields(searchStageDocument);
        BsonDocument autoCompleteOperatorDocument = new BsonDocument();
        if (query.size() == 1) {
            autoCompleteOperatorDocument.append("query", new BsonString(query.get(0)));
        } else {
            autoCompleteOperatorDocument.append("query", query.stream().map(BsonString::new).collect(BsonArray::new, BsonArray::add, BsonArray::addAll));
        }

        appendPath(autoCompleteOperatorDocument, path);

        appendScore(autoCompleteOperatorDocument, score);

        if (tokenOrder != null) {
            autoCompleteOperatorDocument.append("tokenOrder", new BsonString(tokenOrder.key));
        }

        if (fuzzy != null) {
            autoCompleteOperatorDocument.append("fuzzy", fuzzy.toBsonValue());
        }

        searchStageDocument.append("autocomplete", autoCompleteOperatorDocument);

        return searchStageDocument;
    }}
