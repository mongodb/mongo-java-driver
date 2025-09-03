/*
 * Copyright 2008-present MongoDB, Inc.
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

import com.mongodb.client.model.Collation;
import com.mongodb.internal.bulk.UpdateRequest;
import org.bson.BsonDocumentWrapper;
import org.bson.codecs.Encoder;

class ReplaceRequest extends WriteRequest {
    private final DBObject query;
    private final DBObject document;
    private final boolean upsert;
    private final Encoder<DBObject> codec;
    private final Encoder<DBObject> replacementCodec;
    private final Collation collation;

    ReplaceRequest(final DBObject query, final DBObject document, final boolean upsert, final Encoder<DBObject> codec,
                          final Encoder<DBObject> replacementCodec, final Collation collation) {
        this.query = query;
        this.document = document;
        this.upsert = upsert;
        this.codec = codec;
        this.replacementCodec = replacementCodec;
        this.collation = collation;
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

    public Collation getCollation() {
        return collation;
    }

    @Override
    com.mongodb.internal.bulk.WriteRequest toNew(final DBCollection dbCollection) {
        return new UpdateRequest(new BsonDocumentWrapper<>(query, codec),
                new BsonDocumentWrapper<>(document, replacementCodec),
                                                       com.mongodb.internal.bulk.WriteRequest.Type.REPLACE)
               .upsert(isUpsert())
               .collation(getCollation());
    }
}
