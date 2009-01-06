// StringMap.java

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

package ed.util;

import java.util.*;

/** @expose */
public class StringMap<T> extends CustomHashMap<String,T>{

    /** Initializes a new string hash map. */
    public StringMap(){
        super();
    }

    /** Initializes a new string hash map with a given initial capacity and a default load factor of .75.
     * @param initialCap The map's initial capacity
     */
    public StringMap( int initialCap ) {
        super( initialCap );
    }

    /** Initializes a new string hash map with a given initial capacity and a default load factor of .75.
     * @param initialCap The map's initial capacity
     * @param loadFactor The map's load factor
     */
    public StringMap( int initialCap , float loadFactor ) {
        super( initialCap , loadFactor );
    }

    /** Initializes a new string hash map from an existing one.
     * @param m The map to use
     */
    public StringMap( Map<String,? extends T> m ) {
        super( m );
    }

    /** Finds the hash of a string.
     * @param s string for which to find the hash
     * @return the hash code
     */
    public final int doHash( Object s ) {
        return s instanceof String ? Hash.lowerCaseHash( (String)s ) : 0;
    }

    /** Figures out if two object are equal.
     * @param a first object
     * @param b second object
     * @return if the objects are equal
     */
    public final boolean doEquals( Object a , Object b ) {
        return ( ! ( a instanceof String ) || ! ( b instanceof String ) ) ? false : ((String)a).equalsIgnoreCase( (String)b );
    }
}
