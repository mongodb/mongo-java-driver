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

// DBRefBase.java

package com.mongodb;

import org.mongodb.DBRef;

import java.io.Serializable;

/**
 * Represents a database reference, which points to an object stored in the database.
 * <p>
 * While instances of this class are {@code Serializable}, deserialized instances can not be fetched,
 * as the {@code db} property is transient.
 */
public class DBRefBase implements Serializable {

    private static final long serialVersionUID = 3031885741395465814L;

    private final transient DB db;
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
     * For use only with serialization framework.
     */
    protected DBRefBase() {
        proxied = null;
        db = null;
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
     * @return the name of the collection in which the reference is stored
     */
    public String getRef() {
        return proxied.getRef();
    }

    /**
     * Gets the database, which may be null, in which case the reference can not be fetched.
     *
     * @return the database
     * @see #fetch()
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
            throw new MongoInternalException("no db");
        }

        return db.getCollectionFromString(proxied.getRef()).findOne(proxied.getId());
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DBRefBase)) {
            return false;
        }

        DBRefBase dbRefBase = (DBRefBase) o;

        if (proxied != null ? !proxied.equals(dbRefBase.proxied) : dbRefBase.proxied != null) {
            return false;
        }

        return true;
    }

    DBRef toNew() {
        return proxied;
    }

    @Override
    public int hashCode() {
        return proxied.hashCode();
    }

    @Override
    public String toString() {
        return String.format("{\"$ref\":\"%s\",\"$id\":\"%s\"}", proxied.getRef(), proxied.getId());
    }
}
