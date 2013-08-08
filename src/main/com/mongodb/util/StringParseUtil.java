// StringParseUtil.java

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

/**
 * @deprecated This class is NOT a part of public API and will be dropped in 3.x versions.
 */
@Deprecated
public final class StringParseUtil {

    /** Turns a string into a boolean value and returns a default value if unsuccessful.
     * @param s the string to convert
     * @param d the default value
     * @return equivalent boolean value
     */
    public static boolean parseBoolean( String s , boolean d ){

        if ( s == null )
            return d;

        s = s.trim();
        if ( s.length() == 0 )
            return d;

        char c = s.charAt( 0 );

        if ( c == 't' || c == 'T' ||
             c == 'y' || c == 'Y' )
            return true;

        if ( c == 'f' || c == 'F' ||
             c == 'n' || c == 'N' )
            return false;

        return d;
    }

    /** Turns a string into an int and returns a default value if unsuccessful.
     * @param s the string to convert
     * @param def the default value
     * @return the int value
     */
    public static int parseInt( String s , int def ){
        return parseInt( s , def , null , true );
    }

    /** Turns a string into an int using a given radix.
     * @param s the string to convert
     * @param radix radix to use
     * @return the int value
     */
    public static Number parseIntRadix( String s , int radix ){
        if ( s == null )
            return Double.NaN;

        s = s.trim();
        if ( s.length() == 0 )
            return Double.NaN;

        int firstDigit = -1;
        int i = 0;
        if ( s.charAt( 0 ) == '-' )
            i = 1;
        // Find first non-digit.
        for ( ; i<s.length(); i++ ){
            if ( Character.digit( s.charAt( i ) , radix ) == -1 )
                break;
        }

        try {
            // Remember: all numbers in JS are 64-bit
            return Long.valueOf( s.substring( 0, i ) , radix );
        }
        catch (Exception e) {
            return Double.NaN;
        }
    }

    /** Turns a string into an int and returns a default value if unsuccessful.
     * @param s the string to convert
     * @param def the default value
     * @param lastIdx sets lastIdx[0] to the index of the last digit
     * @param allowNegative if negative numbers are valid
     * @return the int value
     */
    public static int parseInt( String s , int def , final int[] lastIdx , final boolean allowNegative ){
        final boolean useLastIdx = lastIdx != null && lastIdx.length > 0;
        if ( useLastIdx )
            lastIdx[0] = -1;

        if ( s == null )
            return def;

        s = s.trim();
        if ( s.length() == 0 )
            return def;

        int firstDigit = -1;
        for ( int i=0; i<s.length(); i++ ){
            if ( Character.isDigit( s.charAt( i ) ) ){
                firstDigit = i;
                break;
            }
        }

        if ( firstDigit < 0 )
            return def;

        int lastDigit = firstDigit + 1;
        while ( lastDigit < s.length() && Character.isDigit( s.charAt( lastDigit ) ) )
            lastDigit++;

        if ( allowNegative && firstDigit > 0 && s.charAt( firstDigit - 1 ) == '-' )
            firstDigit--;

        if ( useLastIdx )
            lastIdx[0] = lastDigit;
        return Integer.parseInt( s.substring( firstDigit , lastDigit ) );
    }

    /** Turns a string into a Number and returns a default value if unsuccessful.
     * @param s the string to convert
     * @param def the default value
     * @return the numeric value
     */
    public static Number parseNumber( String s , Number def ){
        if ( s == null )
            return def;

        s = s.trim();
        if ( s.length() == 0)
            return def;


        int firstDigit = -1;
        for ( int i=0; i<s.length(); i++ ){
            if ( Character.isDigit( s.charAt( i ) ) ){
                firstDigit = i;
                break;
            }
        }

        if ( firstDigit < 0 )
            return def;

        int lastDigit = firstDigit + 1;
        while ( lastDigit < s.length() && Character.isDigit( s.charAt( lastDigit ) ) )
            lastDigit++;
        
        boolean isDouble = false;

        if ( firstDigit > 0 && s.charAt( firstDigit - 1 ) == '.' ){
            firstDigit--;        
            isDouble = true;
        }
        
        if ( firstDigit > 0 && s.charAt( firstDigit - 1 ) == '-' )
            firstDigit--;

        if ( lastDigit < s.length() && s.charAt( lastDigit ) == '.' ){
            lastDigit++;
            while ( lastDigit < s.length() && Character.isDigit( s.charAt( lastDigit ) ) )
                lastDigit++;
            
            isDouble = true;
        }

        if ( lastDigit < s.length() && s.charAt( lastDigit ) == 'E' ){
            lastDigit++;
            while ( lastDigit < s.length() && Character.isDigit( s.charAt( lastDigit ) ) )
                lastDigit++;
            
            isDouble = true;
        }
	

	final String actual = s.substring( firstDigit , lastDigit );

        if ( isDouble || actual.length() > 17  )
            return Double.valueOf( actual );


	if ( actual.length() > 10 )
	    return Long.valueOf(  actual );

        return Integer.valueOf( actual );
    }

    /** Use Java's "strict parsing" methods Integer.parseInt and  Double.parseDouble to parse s "strictly". i.e. if it's neither a double or an integer, fail.
     * @param s the string to convert
     * @return the numeric value
     */
    public static Number parseStrict( String s ){
        if( s.length() == 0 )
            return 0;
        if( s.charAt(0) == '+' ) 
            s = s.substring( 1 );

        if( s.matches( "(\\+|-)?Infinity" ) ) {
            if( s.startsWith( "-" ) ) {
                return Double.NEGATIVE_INFINITY;
            }
            else {
                return Double.POSITIVE_INFINITY;
            }
        }
        else if( s.indexOf('.') != -1 || 
                 s.equals( "-0" ) ) {
            return Double.valueOf(s);
        }
        // parse hex
        else if( s.toLowerCase().indexOf( "0x" ) > -1 ) {
            int coef = s.charAt( 0 ) == '-' ? -1 : 1;
            if( s.length() > 17 ) 
                throw new RuntimeException( "Can't handle a number this big: "+s );
            // if coef == -1: (coef * -.5 + 2.5) == 3
            // e.g., -0xf00 (start substring at 3)
            // if coef == 1: (coef * -.5 + 2.5) == 2
            // e.g., 0xf00 (start substring at 2)
            if( s.length() > 9 )
                return coef * Long.valueOf( s.substring( (int)(coef * -.5 + 2.5) ) , 16 );
            return coef * Integer.valueOf( s.substring( (int)(coef * -.5 + 2.5) ) , 16 );
        }

        int e = s.toLowerCase().indexOf( 'e' );
        // parse exp
        if( e > 0 ) {
            double num = Double.parseDouble( s.substring( 0, e ) );
            int exp = Integer.parseInt( s.substring( e + 1 ) );
            return num * Math.pow( 10 , exp );
        }

        // parse with smallest possible precision
        if ( s.length() > 17 )
            return Double.valueOf( s );
        else if ( s.length() > 9  )
            return Long.valueOf(s);
        return Integer.valueOf(s);
    }

    public static int parseIfInt( String s , int def ){
        if ( s == null || s.length() == 0 )
            return def;

        s = s.trim();
        
        for ( int i=0; i<s.length(); i++ )
            if ( ! Character.isDigit( s.charAt(i) ) )
                return def;
        
        return Integer.parseInt( s );
    }
    
}
