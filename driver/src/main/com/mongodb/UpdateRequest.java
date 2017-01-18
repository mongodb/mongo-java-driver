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

import com.mongodb.client.model.Collation;
import org.bson.BsonDocumentWrapper;
import org.bson.codecs.Encoder;

class UpdateRequest extends WriteRequest {
    private final DBObject query;
    private final DBObject update;
    private final boolean multi;
    private final boolean upsert;
    private final Encoder<DBObject> codec;
    private final Collation collation;

    UpdateRequest(final DBObject query, final DBObject update, final boolean multi, final boolean upsert,
                  final Encoder<DBObject> codec, final Collation collation) {
        this.query = query;
        this.update = update;
        this.multi = multi;
        this.upsert = upsert;
        this.codec = codec;
        this.collation = collation;
    }

    public DBObject getQuery() {
        return query;
    }

    public DBObject getUpdate() {
        return update;
    }

    public boolean isUpsert() {
        return upsert;
    }

    public boolean isMulti() {
        return multi;
    }

    public Collation getCollation() {
        return collation;
    }

    @Override
    com.mongodb.bulk.WriteRequest toNew() {
        return new com.mongodb.bulk.UpdateRequest(new BsonDocumentWrapper<DBObject>(query, codec),
                                                       new BsonDocumentWrapper<DBObject>(update, codec),
                                                       com.mongodb.bulk.WriteRequest.Type.UPDATE)
               .upsert(isUpsert())
               .multi(isMulti())
               .collation(getCollation());
    }
}
