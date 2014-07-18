/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb.operation;

import org.bson.BsonDocument;

public abstract class FindAndModify extends Query {
    private BsonDocument filter;
    private BsonDocument selector;
    private BsonDocument sortCriteria;
    private boolean returnNew;
    private boolean upsert;

    public FindAndModify() {
    }

    public FindAndModify where(final BsonDocument filter) {
        this.filter = filter;
        return this;
    }

    public FindAndModify select(final BsonDocument selector) {
        this.selector = selector;
        return this;
    }

    public FindAndModify sortBy(final BsonDocument sortCriteria) {
        this.sortCriteria = sortCriteria;
        return this;
    }

    public FindAndModify returnNew(final boolean returnNew) {
        this.returnNew = returnNew;
        return this;
    }

    // TODO: doesn't make sense for find and remove
    public FindAndModify upsert(final boolean upsert) {
        this.upsert = upsert;
        return this;
    }

    public BsonDocument getFilter() {
        return filter;
    }

    public BsonDocument getSelector() {
        return selector;
    }

    public BsonDocument getSortCriteria() {
        return sortCriteria;
    }

    // TODO: Doesn't make sense for find and remove
    public boolean isReturnNew() {
        return returnNew;
    }

    public boolean isUpsert() {
        return upsert;
    }
}
