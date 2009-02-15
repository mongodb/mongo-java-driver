// StringMap.java

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

package com.mongodb.util;

import java.util.*;

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
