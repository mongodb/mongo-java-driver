// StringUtil.java

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

public final class StringUtil{

    /**
     * Given two strings, gives the index of the first substring of the first string that matches the second string.
     * @param big the main string
     * @param small the string to find within the main string
     * @param ignoreCase if the search should be case insensitive
     * @param start the index of the main string at which to start searching.
     * @return the index at which a matching substring starts
     */
    public static int indexOf( String big , String small , boolean ignoreCase , int start ){
        for ( int i=start; i< ( big.length() - small.length() ) ; i++ )
            if ( big.regionMatches( ignoreCase , i , small , 0 , small.length() ) )
                return i;
        return -1;
    }

    /**
     * Counts the number of non-overlapping substrings that match a given string.
     * @param big the main string
     * @param small the substring to match.
     * @return the number of matches
     */
    public static int count( String big , String small ){
        int c = 0;
        int idx = 0;

        while ( ( idx = big.indexOf( small , idx ) ) >= 0 ){
            c++;
            idx += small.length();
        }
        return c;
    }

    /**
     * Replace a substring with a different string.
     *
     * @param str String on which to do replacement
     * @param from substring to find and replace
     * @param to string with which to replace <tt>from</tt>
     * @return string w/ replacement if from is found
     */
    public static String replace(String str, String from, String to) {
        if (from == null || from.length() == 0)
            return str;

        StringBuffer buf = null;
        int idx;
        int start = 0;
        while ((idx = str.indexOf(from, start)) >= 0) {
            if (buf == null)
                buf = new StringBuffer();
            buf.append(str.substring(start, idx));
            buf.append(to);
            start = idx + from.length();
        }
        if (buf == null)
            return str;

        buf.append(str.substring(start));

        return buf.toString();
    }

    public static boolean isDigits(final String s) {
        for (int i = 0; i < s.length(); i++)
            if (!Character.isDigit(s.charAt(i)))
                return false;
        return true;
    }
}
