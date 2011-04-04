// Response.java

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

import java.io.*;
import java.util.*;

import org.bson.*;
import org.bson.io.*;

class Response {

    Response( ServerAddress addr , DBCollection collection ,  InputStream in, BSONDecoder decoder)
        throws IOException {

        _host = addr;

        byte[] b = new byte[36];
        Bits.readFully(in, b);

        ByteArrayInputStream bin = new ByteArrayInputStream( b );
        _len = Bits.readInt( bin );
        if ( _len > ( 32 * 1024 * 1024 ) )
        	throw new IllegalArgumentException( "response too long: " + _len );

        _id = Bits.readInt( bin );
        _responseTo = Bits.readInt( bin );
        _operation = Bits.readInt( bin );
        _flags = Bits.readInt( bin );
        _cursor = Bits.readLong( bin );
        _startingFrom = Bits.readInt( bin );
        _num = Bits.readInt( bin );

        MyInputStream user = new MyInputStream( in , _len - b.length );

        if ( _num < 2 )
            _objects = new LinkedList<DBObject>();
        else
            _objects = new ArrayList<DBObject>( _num );

        DBCallback c = collection.getDB().getMongo().getMongoOptions().dbCallbackFactory.create( collection );

        for ( int i=0; i < _num; i++ ){
            if ( user._toGo < 5 )
                throw new IOException( "should have more objects, but only " + user._toGo + " bytes left" );
            c.reset();
            decoder.decode( user , c );

            // TODO: By moving to generics, you can remove these casts (and requirement to impl DBOBject).
            _objects.add( (DBObject)c.get() );
        }

        if ( user._toGo != 0 )
            throw new IOException( "finished reading objects but still have: " + user._toGo + " bytes to read!' " );

        if ( _num != _objects.size() )
            throw new RuntimeException( "something is really broken" );
    }

    public int size(){
        return _num;
    }

    public DBObject get( int i ){
        return _objects.get( i );
    }

    public Iterator<DBObject> iterator(){
        return _objects.iterator();
    }

    public boolean hasGetMore( int queryOptions ){
        if ( _cursor == 0 )
            return false;

        if ( _num > 0 )
            return true;

        if ( ( queryOptions & Bytes.QUERYOPTION_TAILABLE ) == 0 )
            return false;

        // have a tailable cursor

        if ( ( _flags & Bytes.RESULTFLAG_AWAITCAPABLE ) > 0 && ( queryOptions & Bytes.QUERYOPTION_AWAITDATA ) > 0 )
            return true;

        try {
            Thread.sleep( 500 );
        }
        catch ( Exception e ){}

        return true;
    }

    public long cursor(){
        return _cursor;
    }

    public ServerError getError(){
        if ( _num != 1 )
            return null;

        DBObject obj = get(0);

        if ( ServerError.getMsg( obj , null ) == null )
            return null;

        return new ServerError( obj );
    }

    static class MyInputStream extends InputStream {
        MyInputStream( InputStream in , int max ){
            _in = in;
            _toGo = max;
        }

        public int available()
            throws IOException {
            return _in.available();
        }

        public int read()
            throws IOException {

            if ( _toGo <= 0 )
                return -1;

            int val = _in.read();
            _toGo--;

            return val;
        }

        public int read(byte[] b, int off, int len)
            throws IOException {

            if ( _toGo <= 0 )
                return -1;

            int n = _in.read(b, off, Math.min(_toGo, len));
            _toGo -= n;
            return n;
        }

        public void close(){
            throw new RuntimeException( "can't close thos" );
        }

        final InputStream _in;
        private int _toGo;
    }

    public String toString(){
        return "flags:" + _flags + " _cursor:" + _cursor + " _startingFrom:" + _startingFrom + " _num:" + _num ;
    }

    final ServerAddress _host;

    final int _len;
    final int _id;
    final int _responseTo;
    final int _operation;

    final int _flags;
    long _cursor;
    final int _startingFrom;
    final int _num;

    final List<DBObject> _objects;


}
