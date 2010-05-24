// ServerError.java

package com.mongodb;

import org.bson.*;

public class ServerError {
    
    ServerError( DBObject o ){
        Object foo = o.get( "$err" );
        if ( foo == null )
            throw new IllegalArgumentException( "need to have $err" );

        _err = foo.toString();
        _code = _getCode( o );
    }

    static int _getCode( BSONObject o ){
        Object c = o.get( "code" );
        if ( c == null )
            c = o.get( "$code" );
        
        if ( c == null )
            return -5;
        
        return ((Number)c).intValue();
    }
    
    public String getError(){
        return _err;
    }

    public int getCode(){
        return _code;
    }

    public boolean isNotMasterError(){
        return 
            _err.equals( "not master" ) || 
            _code == 10054 || 
            _code == 10056 ||
            _code == 10058 ||
            _code == 10107;
    }

    public String toString(){
        if ( _code > 0 )
            return _code + " " + _err;
        return _err;
    }    

    final String _err;
    final int _code;
}
