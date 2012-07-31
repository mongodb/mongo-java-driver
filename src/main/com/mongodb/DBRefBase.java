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
public class DBRefBase {
    

    /**
     * Creates a DBRefBase
     * @param db the database
     * @param ns the namespace where the object is stored
     * @param id the object id
     */
    public DBRefBase(DB db , String ns , Object id) {
        _db = db;
        _ns = ns.intern();
        _id = id;
    }

    /**
     * fetches the object referenced from the database
     * @return
     * @throws MongoException
     */
    public DBObject fetch() throws MongoException {
        if (_loadedPointedTo)
            return _pointedTo;

        if (_db == null)
            throw new RuntimeException("no db");

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
     * Gets the object's id
     * @return
     */
    public Object getId() {
        return _id;
    }

    /**
     * Gets the object's namespace (collection name)
     * @return
     */
    public String getRef() {
        return _ns;
    }

    /**
     * Gets the database
     * @return
     */
    public DB getDB() {
        return _db;
    }

    @Override
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
    final DB _db;

    private boolean _loadedPointedTo = false;
    private DBObject _pointedTo;
}
