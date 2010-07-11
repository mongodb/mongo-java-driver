// WriteConcern.java

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
/**
 * <p>WriteConcern control the write behavior for with various options, as well as exception raising on error conditions.</p>
 * 
 * w
 *   -1 = don't even report network errors
 *    0 = default, don't call getLastError by default
 *    1 = basic, call getLastError, but don't wait for slaves
 *    2+= wait for slaves
 *
 * wtimeout
 *   how long to wait for slaves before failing
 *   0 = indefinite
 *   > 0 = ms to wait
 *
 * fsync
 *   force fsync to disk
 * @dochub databases
 */
public class WriteConcern {

    public final static WriteConcern NONE = new WriteConcern(-1);
    public final static WriteConcern NORMAL = new WriteConcern(0);
    public final static WriteConcern STRICT = new WriteConcern(1);
    public final static WriteConcern SAFE = new WriteConcern(1);

    public WriteConcern(){
        this(0);
    }

    public WriteConcern( int w ){
        this( w , 0 , false );
    }

    public WriteConcern( int w , int wtimeout ){
        this( w , wtimeout , false );
    }

    public WriteConcern( int w , int wtimeout , boolean fsync ){
        _w = w;
        _wtimeout = wtimeout;
        _fsync = fsync;
        
        _command = new BasicDBObject( "getlasterror" , 1 );
        if ( _w > 0 ){
            _command.put( "w" , _w );
            _command.put( "wtimeout" , wtimeout );
        }

        if ( _fsync )
            _command.put( "fsync" , true );
    }

    public BasicDBObject getCommand(){
        return _command;
    }

    /** @return the number of servers to write to */
    public int getW(){
        return _w;
    }

    /** @return the write timeout (in milliseconds) */
    public int getWtimeout(){
        return _wtimeout;
    }

    /** @return If files are sync'd to disk. */
    public boolean fsync(){
        return _fsync;
    }

    public boolean raiseNetworkErrors(){
        return _w >= 0;
    }

    public boolean callGetLastError(){
        return _w > 0;
    }

    @Override
    public String toString(){
        return "WriteConcern " + _command;
    }

    final int _w; 
    final int _wtimeout;
    final boolean _fsync;

    final BasicDBObject _command;

}
