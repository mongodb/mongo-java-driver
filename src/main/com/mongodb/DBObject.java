// DBObject.java

/**
*    Copyright (C) 2008 10gen Inc.
*  
*    This program is free software: you can redistribute it and/or  modify
*    it under the terms of the GNU Affero General Public License, version 3,
*    as published by the Free Software Foundation.
*  
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU Affero General Public License for more details.
*  
*    You should have received a copy of the GNU Affero General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package com.mongodb;

import java.util.*;

/** A key-value map that can be saved to the database. */
public interface DBObject {

    /** Sets a name/value pair in this object.
     * @param key Name to set
     * @param v Corresponding value
     * @return <tt>v</tt>
     */
    public Object put( String key , Object v );
    
    /** Gets a field from this object by a given name.
     * @param key The name of the field fetch
     * @return The field, if found
     */
    public Object get( String key );

    /** Remove a field with a given name from this object.
     * @param key The name of the field to remove
     * @return The value removed from this object
     */
    public Object removeField( String key );

    /** Checks if this object contains a field with the given name.
     * @param s Field name for which to check
     * @return if this object contains a field with the given name
     */
    public boolean containsKey( String s );

    /** Returns this object's fields' names
     * @return The names of the fields in this object
     */
    public Set<String> keySet();

    public boolean isPartialObject();
    public void markAsPartialObject();
}
