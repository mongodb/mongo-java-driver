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

import com.mongodb.util.*;

/**
 * A simple implementation of <code>DBObject</code>.  
 * A <code>DBObject</code> can be created as follows, using this class:
 * <blockquote><pre>
 * DBObject obj = new BasicDBObject();
 * obj.put( "foo", "bar" );
 * </pre></blockquote>
 */
public class BasicDBObject extends LinkedHashMap<String,Object> implements DBObject {
    
    /**
     *  Creates an empty object.
     */
    public BasicDBObject(){
    }

    /**
     * Convenience CTOR
     * @param key  key under which to store
     * @param value value to stor
     */
    public BasicDBObject(String key, Object value){
        put(key, value);
    }

    /**
     * Creates a DBObject from a map.
     * @param m map to convert
     */
    public BasicDBObject(Map m) {
        super(m);
    }

    /**
     * Converts a DBObject to a map.
     * @return the DBObject
     */
    public Map toMap() {
        return new LinkedHashMap<String,Object>(this);
    }

    /** Deletes a field from this object. 
     * @param key the field name to remove
     * @return the object removed
     */
    public Object removeField( String key ){
        return remove( key );
    }

    /** Checks if this object is ready to be saved.
     * @return if the object is incomplete
     */
    public boolean isPartialObject(){
        return _isPartialObject;
    }

    /** Checks if this object contains a given field
     * @param key field name
     * @return if the field exists
     */
    public boolean containsField( String field ){
        return super.containsKey(field);
    }

    /**
     * @deprecated
     */
    public boolean containsKey( String key ){
        return containsField(key);
    }

    /** Gets a value from this object
     * @param key field name
     * @return the value
     */
    public Object get( String key ){
        return super.get(key);
    }

    /** Returns the value of a field as an <code>int</code>.
     * @param key the field to look for
     * @return the field value (or default)
     */
    public int getInt( String key ){
        return ((Number)get( key )).intValue();
    }

    /** Returns the value of a field as an <code>int</code>.
     * @param key the field to look for
     * @param def the default to return
     * @return the field value (or default)
     */
    public int getInt( String key , int def ){
        Object foo = get( key );
        if ( foo == null )
            return def;
        return ((Number)foo).intValue();
    }

    /**
     * Returns the value of a field as a <code>long</code>.
     *
     * @param key the field to return
     * @return the field value 
     */
    public long getLong( String key){
        Object foo = get( key );
        return ((Number)foo).longValue();
    }


    /** Returns the value of a field as a string
     * @param key the field to look up
     * @return the value of the field, converted to a string
     */
    public String getString( String key ){
        Object foo = get( key );
        if ( foo == null )
            return null;
        return foo.toString();
    }

    /** Add a key/value pair to this object
     * @param key the field name
     * @param val the field value
     * @return the <code>val</code> parameter
     */
    public Object put( String key , Object val ){
        return super.put( key , val );
    }

    public void putAll( Map m ){
        for ( Object k : m.keySet() ){
            put( k.toString() , m.get( k ) );
        }
    } 
    
    public void putAll( DBObject o ){
        for ( String k : o.keySet() ){
            put( k , o.get( k ) );
        }
   } 

    /** Add a key/value pair to this object
     * @param key the field name
     * @param val the field value
     * @return the <code>val</code> parameter
     */
    public BasicDBObject append( String key , Object val ){
        put( key , val );

        return this;
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

    public boolean equals( Object o ){
        if ( ! ( o instanceof DBObject ) )
            return false;
        
        DBObject other = (DBObject)o;
        if ( ! keySet().equals( other.keySet() ) )
            return false;

        for ( String key : keySet() ){
            Object a = get( key );
            Object b = other.get( key );

            if ( a instanceof Number && b instanceof Number ){
                if ( ((Number)a).doubleValue() != 
                     ((Number)b).doubleValue() )
                    return false;
            }
            else {
                if ( ! a.equals( b ) )
                    return false;
            }
        }
        return true;
    }

    private boolean _isPartialObject = false;
}
