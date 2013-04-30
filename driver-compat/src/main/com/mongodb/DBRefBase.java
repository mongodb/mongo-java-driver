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

// DBRefBase.java

package com.mongodb;

import org.mongodb.DBRef;

/**
 * represents a database reference, which points to an object stored in the database
 */
public class DBRefBase {

    private final DB db;
    private final org.mongodb.DBRef proxied;

    /**
     * Creates a DBRefBase
     *
     * @param db the database
     * @param ns the namespace where the object is stored
     * @param id the object id
     */
    public DBRefBase(final DB db, final String ns, final Object id) {
        proxied = new DBRef(id, ns);
        this.db = db;
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
        return db;
    }


    /**
     * fetches the object referenced from the database
     *
     * @return the document that this references.
     * @throws MongoException
     */
    public DBObject fetch() {
        if (db == null) {
            throw new RuntimeException("no db");
        }

        return db.getCollectionFromString(proxied.getRef()).findOne(proxied.getId());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DBRefBase)) {
            return false;
        }

        DBRefBase dbRefBase = (DBRefBase) o;

        if (db != null ? !db.equals(dbRefBase.db) : dbRefBase.db != null) {
            return false;
        }
        if (proxied != null ? !proxied.equals(dbRefBase.proxied) : dbRefBase.proxied != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = db.hashCode();
        result = 31 * result + proxied.hashCode();
        return result;
    }
}
