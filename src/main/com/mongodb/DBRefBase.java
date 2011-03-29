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
        _ns = ns;
        _id = id;
    }

    /**
     * fetches the object referenced from the database
     * @return
     */
    public DBObject fetch() {
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
    public boolean equals(Object obj) {
        if (obj == this)
            return true;

        if (obj instanceof DBRefBase) {
            DBRefBase ref = (DBRefBase) obj;
            if (_ns.equals(ref.getRef()) && _id.equals(ref.getId()))
                return true;
        }
        return false;
    }

    final Object _id;
    final String _ns;
    final DB _db;

    private boolean _loadedPointedTo = false;
    private DBObject _pointedTo;
}
