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

package org.mongodb;

import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.operation.QueryFlag;

import java.util.EnumSet;

public interface MongoView<T> extends MongoWritableView<T>, MongoReadableView<T>, MongoIterable<T> {

    MongoView<T> cursorFlags(final EnumSet<QueryFlag> flags);

    MongoView<T> find(Document filter);

    MongoView<T> find(ConvertibleToDocument filter);


    MongoView<T> sort(Document sortCriteria);

    MongoView<T> sort(ConvertibleToDocument sortCriteria);


    MongoView<T> skip(int skip);

    MongoView<T> limit(int limit);


    MongoView<T> fields(Document selector);

    MongoView<T> fields(ConvertibleToDocument selector);


    MongoView<T> upsert();

    MongoView<T> withQueryOptions(QueryOptions options);

    MongoView<T> withReadPreference(ReadPreference readPreference);

    MongoView<T> withWriteConcern(WriteConcern writeConcern);
}
