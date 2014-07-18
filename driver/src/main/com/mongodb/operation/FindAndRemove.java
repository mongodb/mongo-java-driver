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

import java.util.concurrent.TimeUnit;

public class FindAndRemove<T> extends FindAndModify {

    @Override
    public FindAndRemove<T> where(final BsonDocument filter) {
        super.where(filter);
        return this;
    }

    @Override
    public FindAndRemove<T> select(final BsonDocument selector) {
        super.select(selector);
        return this;
    }

    @Override
    public FindAndRemove<T> sortBy(final BsonDocument sortCriteria) {
        super.sortBy(sortCriteria);
        return this;
    }

    @Override
    public FindAndRemove<T> returnNew(final boolean returnNew) {
        super.returnNew(returnNew);
        return this;
    }

    @Override
    public FindAndRemove<T> upsert(final boolean upsert) {
        throw new UnsupportedOperationException("Can't upsert a remove");
    }

    @Override
    public FindAndRemove<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
        super.maxTime(maxTime, timeUnit);
        return this;
    }
}
