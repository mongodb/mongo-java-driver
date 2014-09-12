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

package com.mongodb;

import org.bson.BsonDocumentWrapper;
import org.bson.codecs.Codec;

class ReplaceRequest extends WriteRequest {
    private final DBObject query;
    private final DBObject document;
    private final boolean upsert;
    private final DBObjectCodec codec;

    public ReplaceRequest(final DBObject query, final DBObject document, final boolean upsert, final DBObjectCodec codec) {
        this.query = query;
        this.document = document;
        this.upsert = upsert;
        this.codec = codec;
    }

    public DBObject getQuery() {
        return query;
    }

    public DBObject getDocument() {
        return document;
    }

    public boolean isUpsert() {
        return upsert;
    }

    @Override
    com.mongodb.operation.WriteRequest toNew(final Codec<DBObject> codec) {
        return new com.mongodb.operation.ReplaceRequest<DBObject>(new BsonDocumentWrapper<DBObject>(query, this.codec), document)
               .upsert(isUpsert());
    }
}
