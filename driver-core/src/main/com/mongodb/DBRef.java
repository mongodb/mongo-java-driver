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

// DBRef.java

package com.mongodb;

import com.mongodb.lang.Nullable;

import java.io.Serializable;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A representation of a database reference.
 *
 * @mongodb.driver.manual reference/database-references/ Database References
 */
public class DBRef implements Serializable {

    private static final long serialVersionUID = -849581217713362618L;

    private final Object id;
    private final String collectionName;
    private final String databaseName;

    /**
     * Construct an instance.
     *
     * @param collectionName the name of the collection where the document is stored
     * @param id             the object id
     */
    public DBRef(final String collectionName, final Object id) {
        this(null, collectionName, id);
    }

    /**
     * Construct an instance.
     *
     * @param databaseName the name of the database where the document is stored
     * @param collectionName the name of the collection where the document is stored
     * @param id             the object id
     * @since 3.3
     */
    public DBRef(@Nullable final String databaseName, final String collectionName, final Object id) {
        this.id = notNull("id", id);
        this.collectionName = notNull("collectionName", collectionName);
        this.databaseName = databaseName;
    }

    /**
     * Gets the _id of the referenced document
     *
     * @return the _id of the referenced document
     */
    public Object getId() {
        return id;
    }

    /**
     * Gets the name of the collection in which the referenced document is stored.
     *
     * @return the name of the collection in which the referenced is stored
     */
    public String getCollectionName() {
        return collectionName;
    }

    /**
     * Gets the name of the database in which the referenced document is stored.  A null value implies that the referenced document is
     * stored in the same database as the referring document.
     *
     * @return the possibly-null database name
     * @since 3.3
     */
    @Nullable
    public String getDatabaseName() {
        return databaseName;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DBRef dbRef = (DBRef) o;

        if (!id.equals(dbRef.id)) {
            return false;
        }
        if (!collectionName.equals(dbRef.collectionName)) {
            return false;
        }
        if (databaseName != null ? !databaseName.equals(dbRef.databaseName) : dbRef.databaseName != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + collectionName.hashCode();
        result = 31 * result + (databaseName != null ? databaseName.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "{ "
                       + "\"$ref\" : \"" + collectionName + "\", \"$id\" : \"" + id + "\""
                       + (databaseName == null ? "" : ", \"$db\" : \"" + databaseName + "\"")
                       + " }";
    }
}
