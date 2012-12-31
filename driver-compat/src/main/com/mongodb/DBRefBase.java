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

import org.mongodb.DBRef;

/**
 * represents a database reference, which points to an object stored in the database
 */
public class DBRefBase {

    private final DB _db;
    private final org.mongodb.DBRef proxied;

    /**
     * Creates a DBRefBase
     *
     * @param db the database
     * @param ns the namespace where the object is stored
     * @param id the object id
     */
    public DBRefBase(DB db, String ns, Object id) {
        proxied = new DBRef(id, ns);
        _db = db;
    }

    /**
     * Gets the object's id
     *
     * @return the id of the referenced document
     */
    public Object getId() {
        return proxied.getId();
    }

    /**
     * Gets the document's collection name.
     *
     * @return the name of the collection that the reference document is in
     */
    public String getRef() {
        return proxied.getRef();
    }

    /**
     * Gets the database
     *
     * @return the database
     */
    public DB getDB() {
        return _db;
    }


    /**
     * fetches the object referenced from the database
     *
     * @return the document that this references.
     * @throws MongoException
     */
    public DBObject fetch() throws MongoException {
        if (_db == null) {
            throw new RuntimeException("no db");
        }

        return _db.getCollectionFromString(proxied.getRef()).findOne(proxied.getId());
    }
}
