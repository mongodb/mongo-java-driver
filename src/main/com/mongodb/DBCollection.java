/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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
 *
 */

package com.mongodb;

import org.mongodb.MongoCollection;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.MongoRemove;
import org.mongodb.operation.MongoInsert;
import org.mongodb.result.InsertResult;
import org.mongodb.result.RemoveResult;

import java.util.Arrays;
import java.util.List;

public class DBCollection {
    private final MongoCollection<DBObject> collection;
    private final DB database;
    private volatile ReadPreference readPreference;
    private volatile WriteConcern writeConcern;

    DBCollection(MongoCollection<DBObject> collection, DB database) {
        this.collection = collection;
        this.database = database;
    }

    public WriteResult insert(DBObject document, WriteConcern writeConcern) {
        return insert(Arrays.asList(document), writeConcern);
    }

    public WriteResult insert(DBObject... documents) {
        return insert(Arrays.asList(documents), getWriteConcern());
    }

    public WriteResult insert(WriteConcern writeConcern, DBObject... documents) {
        return insert(documents, writeConcern);
    }

    public WriteResult insert(DBObject[] documents, WriteConcern writeConcern) {
        return insert(Arrays.asList(documents), writeConcern);
    }

    public WriteResult insert(List<DBObject> documents) {
        return insert(documents, getWriteConcern());
    }

    public WriteResult insert(List<DBObject> documents, WriteConcern writeConcern) {
        MongoInsert<DBObject> insert = new MongoInsert<DBObject>(documents).writeConcern(writeConcern.toNew());
        InsertResult result = collection.insert(insert);
        return new WriteResult(result, writeConcern.toNew());
    }


    public WriteResult remove(DBObject filter, WriteConcern writeConcern) {
    MongoRemove remove = new MongoRemove(DBObjects.toQueryFilterDocument(filter));
        RemoveResult result = collection.remove(remove);
        return new WriteResult(result, writeConcern.toNew());
    }

    public DBCursor find(DBObject filter, DBObject fields) {
        return new DBCursor(collection,
                new MongoFind().
                        where(DBObjects.toQueryFilterDocument(filter)).
                        select(DBObjects.toFieldSelectorDocument(fields)).
                        readPreference(getReadPreference().toNew()));
    }

    public ReadPreference getReadPreference() {
        return readPreference != null ? readPreference : database.getReadPreference();
    }

    public WriteConcern getWriteConcern() {
        return writeConcern != null ? writeConcern : database.getWriteConcern();
    }
}