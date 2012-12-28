/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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
 *
 */

package org.mongodb.operation;

/**
 *
 */
public abstract class MongoFindAndModify extends MongoQuery {
    private MongoQueryFilter filter;
    private MongoFieldSelector selector;
    private MongoSortCriteria sortCriteria;
    private boolean returnNew;
    private boolean upsert;

    public MongoFindAndModify() {}

    public MongoFindAndModify where(final MongoQueryFilter filter) {
        this.filter = filter;
        return this;
    }

    public MongoFindAndModify select(final MongoFieldSelector selector) {
        this.selector = selector;
        return this;
    }

    public MongoFindAndModify sortBy(final MongoSortCriteria sortCriteria) {
        this.sortCriteria = sortCriteria;
        return this;
    }

    public MongoFindAndModify returnNew(final boolean returnNew) {
        this.returnNew = returnNew;
        return this;
    }

    // TODO: doesn't make sense for find and remove
    public MongoFindAndModify upsert(final boolean upsert) {
        this.upsert = upsert;
        return this;
    }

    public MongoQueryFilter getFilter() {
        return filter;
    }

    public MongoFieldSelector getSelector() {
        return selector;
    }

    public MongoSortCriteria getSortCriteria() {
        return sortCriteria;
    }

    public boolean isRemove() {
        return false;
    }

    // TODO: Doesn't make sense for find and remove
    public boolean isReturnNew() {
        return returnNew;
    }

    public boolean isUpsert() {
        return upsert;
    }
}
