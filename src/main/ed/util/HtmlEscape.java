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
