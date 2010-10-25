// BasicDBObject.java

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

import java.util.*;

import org.bson.*;

import com.mongodb.util.*;

/**
 * A simple implementation of <code>DBObject</code>.  
 * A <code>DBObject</code> can be created as follows, using this class:
 * <blockquote><pre>
 * DBObject obj = new BasicDBObject();
 * obj.put( "foo", "bar" );
 * </pre></blockquote>
 */
public class BasicDBObject extends BasicBSONObject implements DBObject {
    
    /**
     *  Creates an empty object.
     */
    public BasicDBObject(){
    }
    
    public BasicDBObject(int size){
    	super(size);
    }

    /**
     * Convenience CTOR
     * @param key  key under which to store
     * @param value value to stor
     */
    public BasicDBObject(String key, Object value){
        super(key, value);
    }

    /**
     * Creates a DBObject from a map.
     * @param m map to convert
     */
    public BasicDBObject(Map m) {
        super(m);
    }

    /** Checks if this object is ready to be saved.
     * @return if the object is incomplete
     */
    public boolean isPartialObject(){
        return _isPartialObject;
    }

    /** Returns a JSON serialization of this object
     * @return JSON serialization
     */    
    public String toString(){
        return JSON.serialize( this );
    }

    /** Sets that this object is incomplete and should not be saved.
     */
    public void markAsPartialObject(){
        _isPartialObject = true;
    }

    /** {@inheritDoc} */
    @Override
    public BasicDBObject append( String key , Object val ){
        put( key , val );
        return this;
    }


    private boolean _isPartialObject = false;
}
