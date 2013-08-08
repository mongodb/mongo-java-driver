// Args.java

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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * @deprecated This class is NOT a part of public API and will be dropped in 3.x versions.
 */
@Deprecated
public class Args {
    public Args( String args[] ){
        
        for ( String s : args ){
            
            if ( s.startsWith( "-" ) ){
                s = s.substring(1);
                int idx = s.indexOf( "=" );
                if ( idx < 0 )
                    _options.put( s , "" );
                else
                    _options.put( s.substring( 0 , idx ) , s.substring( idx + 1 ) );
                continue;
            }
            
            _params.add( s );
            
        }
    }

    public String getOption( String name ){
        return _options.get( name );
    }

    public String toString(){
        StringBuilder s = new StringBuilder();

        for ( String p : _options.keySet() ){
            s.append( '-' ).append( p );

            String v = _options.get( p );
            if ( v.length() == 0 )
                continue;

            s.append( '=' );

            if ( v.indexOf( " " ) >= 0 )
                s.append( '"' ).append( v ).append( '"' );
            else
                s.append( v );
        }

        for ( String p : _params ){
            s.append( ' ' );
            if ( p.indexOf( " " ) >= 0 )
                s.append( '"' ).append( p ).append( '"' );
            else
                s.append( p );
        }

        return s.toString();
    }

    final Map<String,String> _options = new HashMap<String,String>();
    final List<String> _params = new ArrayList<String>();
}
