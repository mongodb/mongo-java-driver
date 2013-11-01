/*
 * Copyright (c) 2008 - 2014 MongoDB Inc. <http://mongodb.com>
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

import static com.mongodb.DBObjects.toDBObject;

class UpdateRequest extends WriteRequest {
    private final DBObject query;
    private final DBObject update;
    private final boolean multi;
    private final boolean upsert;

    public UpdateRequest(final DBObject query, final DBObject update, final boolean multi, final boolean upsert) {
        this.query = query;
        this.update = update;
        this.multi = multi;
        this.upsert = upsert;
    }

    UpdateRequest(final org.mongodb.operation.UpdateRequest updateRequest) {
        this(toDBObject(updateRequest.getFilter()), toDBObject(updateRequest.getUpdateOperations()), updateRequest.isMulti(),
             updateRequest.isUpsert());
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

    @Override
    org.mongodb.operation.WriteRequest toNew() {
        return new org.mongodb.operation.UpdateRequest(DBObjects.toDocument(query), DBObjects.toDocument(update))
               .upsert(isUpsert())
               .multi(isMulti());
    }
}
