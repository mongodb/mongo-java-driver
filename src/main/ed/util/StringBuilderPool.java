// StringBuilderPool.java

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

public class StringBuilderPool extends SimplePool<StringBuilder> {

    /** Initializes a pool of a given number of StringBuilders, each of a certain size.
     * @param maxToKeep the number of string builders in the pool
     * @param maxSize the size of each string builder
     */
    public StringBuilderPool( String name , int maxToKeep , int maxSize ){
        super( "StringBuilderPool-" + name , maxToKeep , -1  );
        _maxSize = maxSize;
    }

    /** Create a new string builder.
     * @return the string builder
     */
    public StringBuilder createNew(){
        return new StringBuilder();
    }

    /** Checks that the given string builder is within the size limit.
     * @param buf the builder to check
     * @return if it is not too big
     */
    public boolean ok( StringBuilder buf ){
        if ( buf.length() > _maxSize )
            return false;
        buf.setLength( 0 );
        return true;
    }

    protected long memSize( StringBuilder buf ){
        return buf.length() * 2;
    }

    final int _maxSize;
}
