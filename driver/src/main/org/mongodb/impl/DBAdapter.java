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

package org.mongodb.impl;

import org.mongodb.MongoDatabase;

//import com.mongodb.CommandResult;
//import com.mongodb.DBObject;
//import org.mongodb.Database;
//
public class DBAdapter {
    @SuppressWarnings("unused")
    private final MongoDatabase impl;

    public DBAdapter(final MongoDatabase database) {
        this.impl = database;
    }
//
//    public Database getDB() {
//        return impl;
//    }
//
//    public DBCollectionAdapter getCollection(final String name) {
//        return new DBCollectionAdapter(impl.getCollection(name));
//    }
//
//    public void requestStart() {
//        throw new UnsupportedOperationException();
//    }
//
//    public void requestDone() {
//        throw new UnsupportedOperationException();
//    }
//
//    public void requestEnsureConnection() {
//        throw new UnsupportedOperationException();
//    }
//
//    public void cleanCursors(final boolean force) {
//        throw new UnsupportedOperationException();
//    }
//
//    public CommandResult executeCommand(final DBObject command) {
//        return impl.executeCommand(command);
//    }
}
