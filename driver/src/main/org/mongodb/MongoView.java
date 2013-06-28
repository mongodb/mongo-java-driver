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
import java.util.concurrent.TimeUnit;

public interface MongoView<T> extends MongoWritableView<T>, MongoAsyncWritableView<T>,
        MongoReadableView<T>, MongoAsyncReadableView<T>, MongoIterable<T> {

    MongoView<T> find(Document filter);

    MongoView<T> find(ConvertibleToDocument filter);

    MongoView<T> sort(Document sortCriteria);

    MongoView<T> sort(ConvertibleToDocument sortCriteria);

    MongoView<T> skip(int skip);

    MongoView<T> limit(int limit);

    MongoView<T> fields(Document selector);

    MongoView<T> fields(ConvertibleToDocument selector);

    MongoView<T> upsert();

    /**
     * To isolate an update from other concurrent updates, by using the "$isolated" special field.
     */
    MongoView<T> withIsolation();

    MongoView<T> withOptions(EnumSet<QueryOption> options);

    MongoView<T> withBatchSize(int batchSize);

    /**
     * Uses "$snapshot".
     */
    MongoView<T> withoutDuplicates();

    MongoView<T> withMaxScan(long count);

    MongoView<T> withMaxTime(long maxTime, TimeUnit timeUnit);

    MongoView<T> withHint(String indexName);

    MongoView<T> withHint(Document index);

    MongoView<T> withHint(ConvertibleToDocument index);

    MongoView<T> withMin(Document max);

    MongoView<T> withMax(Document max);

    MongoView<T> withReadPreference(ReadPreference readPreference);

    MongoView<T> withWriteConcern(WriteConcern writeConcern);

}
