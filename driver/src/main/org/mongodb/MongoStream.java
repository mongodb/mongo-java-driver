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

import org.mongodb.async.MongoAsyncReadableStream;
import org.mongodb.async.MongoAsyncWritableStream;
import org.mongodb.operation.Find;
import org.mongodb.operation.QueryOption;

import java.util.EnumSet;

public interface MongoStream<T> extends MongoSyncWritableStream<T>, MongoAsyncWritableStream<T>,
        MongoSyncReadableStream<T>, MongoAsyncReadableStream<T>, MongoIterable<T> {

    MongoStream<T> batchSize(int batchSize);   // TODO: what to do about this

    MongoStream<T> withOptions(EnumSet<QueryOption> options);

    MongoStream<T> tail();

    MongoStream<T> withReadPreference(ReadPreference readPreference);

    MongoStream<T> withWriteConcern(WriteConcern writeConcern);

    MongoStream<T> find(Document filter);

    MongoStream<T> find(ConvertibleToDocument filter);

    MongoStream<T> sort(Document sortCriteria);

    MongoStream<T> sort(ConvertibleToDocument sortCriteria);

    MongoStream<T> skip(int skip);

    MongoStream<T> limit(int limit);

    MongoStream<T> noLimit();

    MongoStream<T> fields(Document selector);

    MongoStream<T> fields(ConvertibleToDocument selector);

    MongoStream<T> upsert();

    /**
     * Returns a copy of the criteria for this stream.  Modifications to the copy will have no effect on the stream.
     *
     * @return the criteria
     */
    Find getCriteria();
}
