// DBRefBase.java

/**
 *      Copyright (C) 2008 10gen Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.mongodb;

/**
 * represents a database reference, which points to an object stored in the database
 */
public class DBRefBase extends org.mongodb.DBRef {


    /**
     * Creates a DBRefBase
     *
     * @param db the database
     * @param ns the namespace where the object is stored
     * @param id the object id
     */
    public DBRefBase(DB db, String ns, Object id) {
        super(id, ns);
        _db = db;
    }

    /**
     * Gets the database
     *
     * @return
     */
    public DB getDB() {
        return _db;
    }


    /**
     * fetches the object referenced from the database
     *
     * @return
     * @throws MongoException
     */
    public DBObject fetch() throws MongoException {
        if (_loadedPointedTo) {
            return _pointedTo;
        }

        if (_db == null) {
            throw new RuntimeException("no db");
        }

        final DBCollection coll = _db.getCollectionFromString(getRef());

        _pointedTo = coll.findOne(getId());
        _loadedPointedTo = true;
        return _pointedTo;
    }

    final DB _db;

    private boolean _loadedPointedTo = false;
    private DBObject _pointedTo;
}
