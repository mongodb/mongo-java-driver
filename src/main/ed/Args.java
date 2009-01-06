// Args.java

package ed;

import java.util.*;

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
        String s = "";
        
        for ( String p : _options.keySet() ){
            s += "-" + p;

            String v = _options.get( p );
            if ( v.length() == 0 )
                continue;

            s += "=";
            
            if ( v.indexOf( " " ) >= 0 )
                s += "\"" + v + "\"";
            else
                s += v;
        }

        for ( String p : _params ){
            s += " ";
            if ( p.indexOf( " " ) >= 0 )
                s += "\"" + p + "\"";
            else 
                s += p;
        }
            
        
        return s;
    }

    final Map<String,String> _options = new HashMap<String,String>();
    final List<String> _params = new ArrayList<String>();
}
