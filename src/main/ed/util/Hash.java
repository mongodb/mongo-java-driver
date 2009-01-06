// Hash.java

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
public final class Hash {

    /** Creates a hash for a string.
     * @param s String to hash
     * @return the hash code
     */
    public static final int hashBackward( String s ) {
        int hash = 0;
        for ( int i = s.length()-1; i >= 0; i-- )
            hash = hash * 31 + s.charAt( i );
        return hash;
    }

    /** Creates a long hash for a string.
     * @param s the string to hash
     * @return the hash code
     */
    public static final long hashBackwardLong( String s ) {
        long hash = 0;
        for ( int i = s.length()-1; i >= 0; i-- )
            hash = hash * 63 + s.charAt( i );
        return hash;
    }

    /** @unexpose */
    static final long _longHashConstant = 4095;

    /**
     * 64-bit hash, using longs, in stead of ints, for less collisions, for when it matters.
     * Calls longHash( s , 0 , s.length() )
     * @param s         The String to hash.
     * @return the hash code
     */
    public static final long longHash( String s ) {
        return longHash( s , 0 , s.length() );
    }

    /**
     * 64-bit hash using longs, starting on index 'start' and including everything before 'end'.
     * @param s         The string to hash.
     * @param start     Where to start the hash.
     * @param end       Where to end the hash.
     * @return the hash code
     */
    public static final long longHash( String s , int start , int end ) {
        long hash = 0;
        for ( ; start < end; start++ )
            hash = _longHashConstant * hash + s.charAt( start );
        return hash;
    }

    /**
     * Same as longHash(String), using only lower-case values of letters.
     * Calls longhash( s , 0 , s.length() ).
     * @param s     The string to Hash.
     * @return the hash code
     */
    public static final long longLowerHash( String s ) {
        return longLowerHash( s , 0 , s.length() );
    }

    /**
     * Long (64-bit) hash, lower-cased, from [start-end)
     * @param s      The string to hash.
     * @param start  where to start hashing.
     * @param end    Where to stop hashing.
     * @return the hash code
     */
    public static final long longLowerHash( String s , int start , int end ) {
        long hash = 0;
        for ( ; start < end; start++ )
            hash = _longHashConstant * hash + Character.toLowerCase( s.charAt( start ) );
        return hash;
    }

    /**
     * Long (64-bit) hash, lower-cased, from [start-end)
     * @param s      The string to hash.
     * @param start  where to start hashing.
     * @param end    Where to stop hashing.
     * @return the hash code
     */
    public static final long longLowerHash( String s , int start , int end , long hash ) {
        for ( ; start < end; start++ )
            hash = _longHashConstant * hash + Character.toLowerCase( s.charAt( start ) );
        return hash;
    }

    /** Adds the lower-case equivalent of a character to an existing hash code.
     * @param hash the existing hash code
     * @param c the character to add
     * @return the hash code
     */
    public static final long longLowerHashAppend( long hash , char c ) {
        return hash * _longHashConstant + Character.toLowerCase( c );
    }

    /** Adds a character to an existing hash code.
     * @param hash the existing hash code
     * @param c the character to add
     * @return the hash code
     */
    public static final long longHashAppend( long hash , char c ) {
        return hash * _longHashConstant + c;
    }

    /**
     * This is an exact copy of the String <code>hashCode()</code> function, aside from the lowercasing.
     * @param s string to be hashed
     * @return the hash code
     */
    public static final int lowerCaseHash( String s ) {
	int h = 0;
        final int len = s.length();
        for ( int i = 0; i < len; i++ )
            h = 31*h + Character.toLowerCase( s.charAt( i ) );
        return h;
    }

    /**
     * Creates a hash code of a lowercase string from [start-end)
     * @param s string to be hashed
     * @param start the starting index
     * @param end the ending index
     * @return the hash code
     */
    public static final int lowerCaseHash( String s , int start , int end ) {
	int h = 0;
        final int len = s.length();
        for ( int i = start; i < len && i < end; i++ )
            h = 31*h + Character.toLowerCase( s.charAt( i ) );
        return h;
    }

    /**
     * Creates a hash code of a string from [start-end)
     * @param s string to be hashed
     * @param start the starting index
     * @param end the ending index
     * @return the hash code
     */
    public static final int hashCode( CharSequence s , int start , int end ) {
	int h = 0;
        final int len = s.length();
        for ( int i = start; i < len && i < end; i++ )
            h = 31*h + s.charAt( i );
        return h;
    }

    /**
     * Creates a hash code of a lowercase string with whitespace removed from [start-end)
     * @param s string to be hashed
     * @param start the starting index
     * @param end the ending index
     * @return the hash code
     */
    public static final int nospaceLowerHash( String s , int start , int end ) {
	int h = 0;
        final int len = s.length();
        for ( int i = start; i < len && i < end; i++ ) {
            char c = s.charAt( i );
            if ( Character.isWhitespace( c ) )
                continue;
            h = 31*h + Character.toLowerCase( c );
        }
        return h;
    }

    /**
     * This is an exact copy of the String <code>hashCode()</code> function, aside from the lowercasing.
     * No, it's not.  It also ignores consecutive whitespace.
     * @param s string to be hashed
     * @return the hash code
     */
    public static final int lowerCaseSpaceTrimHash( String s ) {
	int h = 0;
        int len = s.length();
        while ( len > 1 && Character.isWhitespace( s.charAt( len-1 ) ) )
            len--;
        boolean lastWasSpace = true;
        for ( int i = 0; i < len; i++ ) {
            boolean isSpace = Character.isWhitespace( s.charAt( i ) );
            if ( isSpace && lastWasSpace )
                continue;
            lastWasSpace = isSpace;
            h = 31*h + Character.toLowerCase( s.charAt( i ) );
        }
        return h;
    }

    /**
     * Creates a hash code of a lowercase string from [start-end) ignoring whitespace
     * @param s string to be hashed
     * @param start the starting index
     * @param end the ending index
     * @return the hash code
     */
    public static final int lowerCaseSpaceTrimHash( String s , int start , int end ) {
	int h = 0;
        int len = s.length();
        while ( len > 1 && Character.isWhitespace( s.charAt( len-1 ) ) )
            len--;
        boolean lastWasSpace = true;
        for ( int i = start; i < len && i < end; i++ ) {
            boolean isSpace = Character.isWhitespace( s.charAt( i ) );
            if ( isSpace && lastWasSpace )
                continue;
            lastWasSpace = isSpace;
            h = 31*h + Character.toLowerCase( s.charAt( i ) );
        }
        return h;
    }

    /**
     * Calculate the hashcode for a series of strings combined as one.
     * @param strings     Varargs array of Strings.
     * @return            A hashcode.
     */
    public static final int hashCode( String ... strings ) {
	int h = 0;
        for ( String s : strings ) {
            int len = s.length();
            for ( int i = 0; i < len; i++ )
                h = 31*h + s.charAt( i );
        }
        return h;
    }

}
