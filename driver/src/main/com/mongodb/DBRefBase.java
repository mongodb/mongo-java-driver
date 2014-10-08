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

import java.io.Serializable;

/**
 * <p>Represents a database reference, which points to an object stored in the database.</p>
 *
 * <p>While instances of this class are {@code Serializable}, deserialized instances can not be fetched, as the {@code db} property is
 * transient.</p>
 *
 * @mongodb.driver.manual applications/database-references Database References
 */
public class DBRefBase implements Serializable {
    private static final long serialVersionUID = 3031885741395465814L;

    private final transient DB db;
    private final Object id;
    private final String collectionName;

    /**
     * Creates a DBRefBase
     *
     * @param db             the database
     * @param collectionName the name of the collection where the object is stored
     * @param id             the object id
     */
    public DBRefBase(final DB db, final String collectionName, final Object id) {
        this.id = id;
        this.collectionName = collectionName;
        this.db = db;
    }

    /**
     * Gets the object's id
     *
     * @return the id of the referenced document
     */
    public Object getId() {
        return id;
    }

    /**
     * Gets the document's collection name.
     *
     * @return the name of the collection in which the reference is stored
     */
    public String getRef() {
        return collectionName;
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
     * Fetches the object referenced from the database
     *
     * @return the document that this references.
     * @throws MongoException
     */
    public DBObject fetch() {
        if (db == null) {
            throw new MongoInternalException("no db");
        }

        return db.getCollectionFromString(collectionName).findOne(id);
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

        if (id != null ? !id.equals(dbRefBase.id) : dbRefBase.id != null) {
            return false;
        }
        if (collectionName != null ? !collectionName.equals(dbRefBase.collectionName) : dbRefBase.collectionName != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (collectionName != null ? collectionName.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "{ \"$ref\" : \"" + collectionName + "\", \"$id\" : \"" + id + "\" }";
    }
}

