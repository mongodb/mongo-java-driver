/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb;

import org.mongodb.async.MongoAsyncReadableView;
import org.mongodb.async.MongoAsyncWritableView;
import org.mongodb.operation.QueryOption;

import java.util.EnumSet;

public interface MongoView<T> extends MongoWritableView<T>, MongoAsyncWritableView<T>,
        MongoReadableView<T>, MongoAsyncReadableView<T>, MongoIterable<T> {

    MongoView<T> batchSize(int batchSize);   // TODO: what to do about this

    MongoView<T> withOptions(EnumSet<QueryOption> options);

    MongoView<T> tail();

    MongoView<T> withReadPreference(ReadPreference readPreference);

    MongoView<T> withWriteConcern(WriteConcern writeConcern);

    MongoView<T> find(Document filter);

    MongoView<T> find(ConvertibleToDocument filter);

    MongoView<T> sort(Document sortCriteria);

    MongoView<T> sort(ConvertibleToDocument sortCriteria);

    MongoView<T> skip(int skip);

    MongoView<T> limit(int limit);

    MongoView<T> noLimit();

    MongoView<T> fields(Document selector);

    MongoView<T> fields(ConvertibleToDocument selector);

    MongoView<T> upsert();
}
