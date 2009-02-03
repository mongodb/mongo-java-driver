// BasicDBObject.java

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
public class BasicDBObject extends HashMap<String,Object> implements DBObject {
    
    /** Creates an empty object. */
    public BasicDBObject(){
    }

    /** Deletes a field from this object. 
     * @param key the field name to remove
     * @param the object removed
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

    /** Checks if this object contains a given key
     * @param key field name
     * @return if the field exists
     */
    public boolean containsKey( String key ){
        return super.containsKey( (Object)key );
    }

    /** Gets a value from this object
     * @param key field name
     * @return the value
     */
    public Object get( String key ){
        return super.get( (Object)key );
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
        _keys.add( key );
        return super.put( key , val );
    }

    /** Gets a set of this object's fieldnames
     * @return the fieldnames
     */
    public Set<String> keySet(){
        assert( _keys.size() == size() );
        return _keys;
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

    private final Set<String> _keys = new OrderedSet<String>();
    private boolean _isPartialObject = false;
}
