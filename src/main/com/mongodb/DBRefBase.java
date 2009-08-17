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
 * Base class for DBRefs.
 */
public class DBRefBase {
    
    static final boolean D = Boolean.getBoolean( "DEBUG.DBREF" );

    public DBRefBase(DBBase db , String ns , ObjectId id) {
        _db = db;
        
        _ns = ns;
        _id = id;
    }

    public DBObject fetch() {
        if (_loadedPointedTo)
            return _pointedTo;

        if (_db == null)
            throw new RuntimeException("no db");

        if (D) {
            System.out.println("following db pointer. ref to ns:" + _ns);
            Throwable t = new Throwable();
            t.fillInStackTrace();
            t.printStackTrace();
        }

        final DBCollection coll = _db.getCollectionFromString(_ns);

        _pointedTo = coll.findOne(_id);
        _loadedPointedTo = true;
        return _pointedTo;
    }

    public String toString(){
        return "{ \"$ref\" : \"" + _ns + "\", \"$id\" : \"" + _id + "\" }";
    }

    final Object _id;
    final String _ns;
    final DBBase _db;

    private boolean _loadedPointedTo = false;
    private DBObject _pointedTo;
}
