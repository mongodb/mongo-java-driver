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
import com.mongodb.internal.bulk.DeleteRequest;
import org.bson.BsonDocumentWrapper;
import org.bson.codecs.Encoder;

class RemoveRequest extends WriteRequest {
    private final DBObject query;
    private final boolean multi;
    private final Encoder<DBObject> codec;
    private final Collation collation;

    RemoveRequest(final DBObject query, final boolean multi, final Encoder<DBObject> codec, final Collation collation) {
        this.query = query;
        this.multi = multi;
        this.codec = codec;
        this.collation = collation;
    }

    public DBObject getQuery() {
        return query;
    }

    public boolean isMulti() {
        return multi;
    }

    @Override
    com.mongodb.internal.bulk.WriteRequest toNew(final DBCollection dbCollection) {
        return new DeleteRequest(new BsonDocumentWrapper<>(query, this.codec)).multi(isMulti()).collation(collation);
    }
}
