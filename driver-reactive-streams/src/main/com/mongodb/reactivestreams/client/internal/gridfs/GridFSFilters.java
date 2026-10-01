/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.reactivestreams.client.internal.gridfs;

import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 * Filters for queries against the GridFS {@code files} and {@code chunks} collections.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
final class GridFSFilters {

    /**
     * Builds a filter that matches {@code fieldName} against {@code id} exactly.
     *
     * <p>The {@code $eq} operator is required because a GridFS file id is an arbitrary user-supplied {@link BsonValue}. Used directly as
     * a filter value, a document such as {@code {$gt: MinKey}} would be interpreted as a query predicate and match files other than the
     * one identified by {@code id}.</p>
     */
    static BsonDocument eq(final String fieldName, final BsonValue id) {
        return new BsonDocument(fieldName, new BsonDocument("$eq", id));
    }

    private GridFSFilters() {
    }
}
