// BasicBSONObject.java

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

package org.bson;

import java.util.*;
import java.util.regex.Pattern;

/**
 * A simple implementation of <code>DBObject</code>.  
 * A <code>DBObject</code> can be created as follows, using this class:
 * <blockquote><pre>
 * DBObject obj = new BasicBSONObject();
 * obj.put( "foo", "bar" );
 * </pre></blockquote>
 */
public class BasicBSONObject extends LinkedHashMap<String,Object> implements BSONObject {

    private static final long serialVersionUID = -4415279469780082174L;
    
    /**
     *  Creates an empty object.
     */
    public BasicBSONObject(){
    }

    public BasicBSONObject(int size){
    	super(size);
    }

    /**
     * Convenience CTOR
     * @param key  key under which to store
     * @param value value to stor
     */
    public BasicBSONObject(String key, Object value){
        put(key, value);
    }

    /**
     * Creates a DBObject from a map.
     * @param m map to convert
     */
    @SuppressWarnings("unchecked")   
    public BasicBSONObject(Map m) {
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

    /** Checks if this object contains a given field
     * @param field field name
     * @return if the field exists
     */
    public boolean containsField( String field ){
        return super.containsKey(field);
    }

    /**
     * @deprecated
     */
    @Deprecated
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
        Object o = get(key);
        if ( o == null )
            throw new NullPointerException( "no value for: " + key );
        
        return BSON.toInt( o );
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
        
        return BSON.toInt( foo );
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

    /**
     * Returns the value of a field as a <code>double</code>.
     *
     * @param key the field to return
     * @return the field value 
     */
    public double getDouble( String key){
        Object foo = get( key );
        return ((Number)foo).doubleValue();
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

    /** Returns the value of a field as a boolean.
     * @param key the field to look up
     * @return the value of the field, or false if field does not exist
     */
    public boolean getBoolean( String key ){
        return getBoolean(key, false);
    }

    /** Returns the value of a field as a boolean
     * @param key the field to look up
     * @param def the default value in case the field is not found
     * @return the value of the field, converted to a string
     */
    public boolean getBoolean( String key , boolean def ){
        Object foo = get( key );
        if ( foo == null )
            return def;
        if ( foo instanceof Number )
            return ((Number)foo).intValue() > 0;
        if ( foo instanceof Boolean )
            return ((Boolean)foo).booleanValue();
        throw new IllegalArgumentException( "can't coerce to bool:" + foo.getClass() );
    }

    /** Add a key/value pair to this object
     * @param key the field name
     * @param val the field value
     * @return the <code>val</code> parameter
     */
    public Object put( String key , Object val ){
        return super.put( key , val );
    }

    @SuppressWarnings("unchecked")   
    public void putAll( Map m ){
        for ( Map.Entry entry : (Set<Map.Entry>)m.entrySet() ){
            put( entry.getKey().toString() , entry.getValue() );
        }
    } 
    
    public void putAll( BSONObject o ){
        for ( String k : o.keySet() ){
            put( k , o.get( k ) );
        }
   } 

    /** Add a key/value pair to this object
     * @param key the field name
     * @param val the field value
     * @return <code>this</code>
     */
    public BasicBSONObject append( String key , Object val ){
        put( key , val );

        return this;
    }

    /** Returns a JSON serialization of this object
     * @return JSON serialization
     */    
    public String toString(){
        return com.mongodb.util.JSON.serialize( this );
    }

    public boolean equals( Object o ){
        if ( ! ( o instanceof BSONObject ) )
            return false;
        
        BSONObject other = (BSONObject)o;
        if ( ! keySet().equals( other.keySet() ) )
            return false;

        for ( String key : keySet() ){
            Object a = get( key );
            Object b = other.get( key );
            
            if ( a == null ){
                if ( b != null )
                    return false;
            }
            if ( b == null ){
                if ( a != null )
                    return false;
            }
            else if ( a instanceof Number && b instanceof Number ){
                if ( ((Number)a).doubleValue() != 
                     ((Number)b).doubleValue() )
                    return false;
            }
            else if ( a instanceof Pattern && b instanceof Pattern ){
                Pattern p1 = (Pattern) a;
                Pattern p2 = (Pattern) b;
                if (!p1.pattern().equals(p2.pattern()) || p1.flags() != p2.flags())
                    return false;
            }
            else {
                if ( ! a.equals( b ) )
                    return false;
            }
        }
        return true;
    }


}
