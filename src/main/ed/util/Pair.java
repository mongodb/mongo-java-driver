// Pair.java

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

/** @expose */
public class Pair<A,B>{

    /** Initializes a pair. */
    public Pair(){
    }

    /** Initializes a pair of objects.
     * @param a First object
     * @param b Second object
     */
    public Pair( A a , B b ){
        first = a;
        second = b;
    }

    /** Find the hash code for the pair.
     * @return the hash code
     */
    public int hashCode(){
        return
            ( first == null ? 0 : first.hashCode() ) +
            ( second == null ? 0 : second.hashCode() );
    }

    /** Figures out whether a given object is equivalent to this pair.
     * @param o object to compare
     * @return if the objects are equal
     */
    public boolean equals( Object o ){
        if ( ! ( o instanceof Pair ) )
            return false;

        Pair other = (Pair)o;
        return _equals( first , other.first ) && _equals( second , other.second );
    }

    /** @unexpose  */
    private final boolean _equals( Object o1 , Object o2 ){
        if ( o1 == null )
            return o2 == null;
        if ( o2 == null )
            return false;

        return o1.equals( o2 );
    }

    public String toString(){
        return "<" + first + "," + second + ">";
    }

    public A first;
    public B second;
}
