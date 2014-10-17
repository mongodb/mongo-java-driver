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
 * @mongodb.driver.manual reference/database-references/ Database References
 * @deprecated Use {@link com.mongodb.DBRef} class exclusively
 */
@Deprecated
public class DBRefBase implements Serializable {
    private static final long serialVersionUID = 3031885741395465814L;

    /**
     * Creates a DBRefBase
     *
     * @param ns the namespace where the object is stored
     * @param id the object id
     */
    public DBRefBase(String ns , Object id) {
        _db = null;
        _ns = ns.intern();
        _id = id;
    }

    /**
     * Creates a DBRefBase
     *
     * @param db the database
     * @param ns the namespace where the object is stored
     * @param id the object id
     *
     * @deprecated Deprecated because {@link #fetch()} is deprecated. Use {@link #DBRefBase(String, Object)} instead
     */
    @Deprecated
    public DBRefBase(DB db , String ns , Object id) {
        _db = db;
        _ns = ns.intern();
        _id = id;
    }

    /**
     * For use only with serialization framework.
     */
    protected DBRefBase() {
        _id = null;
        _ns = null;
        _db = null;
    }

    /**
     * Fetches the object referenced from the database
     *
     * @return the document that this references.
     * @throws MongoException
     * @deprecated use {@link com.mongodb.DBCollection#findOne(Object)} instead
     */
    @Deprecated
    public DBObject fetch() throws MongoException {
        if (_loadedPointedTo)
            return _pointedTo;

        if (_db == null)
            throw new MongoInternalException("no db");

        final DBCollection coll = _db.getCollectionFromString(_ns);

        _pointedTo = coll.findOne(_id);
        _loadedPointedTo = true;
        return _pointedTo;
    }

    @Override
    public String toString(){
        return "{ \"$ref\" : \"" + _ns + "\", \"$id\" : \"" + _id + "\" }";
    }

    /**
     * Gets the _id of the referenced document
     *
     * @return the _id of the referenced document
     */
    public Object getId() {
        return _id;
    }

    /**
     * Gets the name of the collection in which the referenced document is stored.
     *
     * @return the name of the collection in which the referenced is stored
     */
    public String getCollectionName() {
        return _ns;
    }

    /**
     * Gets the object's namespace (collection name)
     * @return the name of the collection in which the reference is stored
     * @deprecated use {@link #getCollectionName()} instead
     */
    @Deprecated
    public String getRef() {
        return _ns;
    }

    /**
     * Gets the database, which may be null, in which case the reference can not be fetched.
     *
     * @return the database
     * @see #fetch()
     * @deprecated deprecated because {@link #fetch()} is deprecated
     */
    @Deprecated
    public DB getDB() {
        return _db;
    }

    @Override
    @SuppressWarnings("deprecation")
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final DBRefBase dbRefBase = (DBRefBase) o;

        if (_id != null ? !_id.equals(dbRefBase._id) : dbRefBase._id != null) return false;
        if (_ns != null ? !_ns.equals(dbRefBase._ns) : dbRefBase._ns != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = _id != null ? _id.hashCode() : 0;
        result = 31 * result + (_ns != null ? _ns.hashCode() : 0);
        return result;
    }

    final Object _id;
    final String _ns;
    final transient DB _db;

    private boolean _loadedPointedTo = false;
    private DBObject _pointedTo;
}
