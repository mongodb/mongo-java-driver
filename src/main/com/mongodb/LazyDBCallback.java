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

import org.bson.LazyBSONCallback;

/**
 *
 */
public class LazyDBCallback extends LazyBSONCallback implements DBCallback {

    public LazyDBCallback( DBCollection coll ){
        _collection = coll;
        _db = _collection == null ? null : _collection.getDB();
    }

    @Override
    public Object createObject(byte[] data, int offset) {
        LazyDBObject o = new LazyDBObject(data, offset, this);
        if ( o.containsField( "$ref" ) &&
             o.containsField( "$id" ) ){
            return new DBRef( _db, o );
        }
        return o;
    }

    final DBCollection _collection;
    final DB _db;
}
