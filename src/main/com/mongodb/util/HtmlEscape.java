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

public class HtmlEscape {

    /** Escapes special HTML characters in a string.  Replaces &amp;, &quot;, &apos;, &lt;, and &gt;.
     * @param s string to escape
     * @return escaped string
     */
    static public String escape( String s ) {
        s = s.replaceAll("&", "&amp;");
        s = s.replaceAll("\\\"", "&quot;");
        s = s.replaceAll("\\\'", "&apos;");
        s = s.replaceAll("<", "&lt;");
        s = s.replaceAll(">", "&gt;");
        return s;
    }
};
