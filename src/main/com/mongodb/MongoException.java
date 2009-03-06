// MongoException.java

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

package com.mongodb;

public class MongoException extends Exception {

    public MongoException( String msg ){
        super( msg );
    }

    public MongoException( String msg , Throwable t ){
        super( msg , _massage( t ) );
    }

    static Throwable _massage( Throwable t ){
        if ( t instanceof Network )
            return ((Network)t)._ioe;
        return t;
    }

    static class Network extends MongoException {

        Network( String msg , java.io.IOException ioe ){
            super( msg , ioe );
            _ioe = ioe;
        }

        Network( java.io.IOException ioe ){
            super( ioe.toString() , ioe );
            _ioe = ioe;
        }
        
        final java.io.IOException _ioe;
    }

}
