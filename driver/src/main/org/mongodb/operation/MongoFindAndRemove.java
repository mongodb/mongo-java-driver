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

public class MongoFindAndRemove extends MongoFindAndModify {
    public boolean isRemove() {
        return true;
    }

    @Override
    public MongoFindAndRemove where(final MongoQueryFilter filter) {
        super.where(filter);
        return this;
    }

    @Override
    public MongoFindAndRemove select(final MongoFieldSelector selector) {
        super.select(selector);
        return this;
    }

    @Override
    public MongoFindAndRemove sortBy(final MongoSortCriteria sortCriteria) {
        super.sortBy(sortCriteria);
        return this;
    }

    @Override
    public MongoFindAndRemove returnNew(final boolean returnNew) {
        super.returnNew(returnNew);
        return this;
    }

    @Override
    public MongoFindAndRemove upsert(final boolean upsert) {
        throw new UnsupportedOperationException("Can't upsert a remove");
    }

}
