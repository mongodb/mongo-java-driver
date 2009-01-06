// Machine.java

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

public class Machine {
    
    public static enum OSType { 
        MAC , LINUX , WIN , OTHER;

        public boolean isMac(){
            return this == MAC;
        }

        public boolean isLinux(){
            return this == LINUX;
        }

    };

    static final OSType _os;
    static {
        OSType me = null;
        String osName = System.getProperty( "os.name" ).toLowerCase();
        if ( osName.indexOf( "linux" ) >= 0 )
            me = OSType.LINUX;
        else if ( osName.indexOf( "mac" ) >= 0 )
            me = OSType.MAC;
        else if ( osName.indexOf( "win" ) >= 0 )
            me = OSType.WIN;
        else {
            System.err.println( "unknown os name [" + osName + "]" );
            me = OSType.OTHER;
        }
        _os = me;
    }
    
    public static OSType getOSType(){
        return _os;
    }
    
}
