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

    Response( DBCollection collection ,  InputStream in )
        throws IOException {
        
        _collection = collection;
        _raw = in;
        
        byte[] b = new byte[16];
        int x = 0;
        while ( x<b.length ){
            x += in.read( b , x , b.length - x );
        }
        
        ByteArrayInputStream bin = new ByteArrayInputStream( b );
        _len = Bits.readInt( bin );
        _id = Bits.readInt( bin );
        _responseTo = Bits.readInt( bin );
        _operation = Bits.readInt( bin );

        if ( _len > ( 12 * 1024 * 1024 ) )
            throw new IllegalArgumentException( "response too long: " + _len );

        _user = new MyInputStream( in , _len - 16 );

        _flags = Bits.readInt( _user );
        _cursor = Bits.readLong( _user );
        _startingFrom = Bits.readInt( _user );
        _num = Bits.readInt( _user );
        
        _readSoFar = 0;

        _decoder = TL.get();
    }

    void addHook( DoneHook h ){
        _hooks.add( h );
    }

    boolean more(){
        return _peek != null || _readSoFar < _num;
    }
    
    DBObject peek(){
        if ( _peek == null && more() ){
            _peek = next();
        }
        return _peek;
    }
    
    DBObject next(){
        if ( _peek != null ){
            DBObject foo = _peek;
            _peek = null;
            return foo;
        }
        if ( _readSoFar >= _num )
            throw new IllegalStateException( "already finished" );

        DBCallback c = new DBCallback( _collection );
        try {
            _decoder.decode( _user , c );
        }
        catch ( IOException ioe ){
            for ( DoneHook h : _hooks )
                h.error( ioe );
            _hooks.clear();
            throw new MongoException.Network( "can't read response" , ioe );
        }
        _readSoFar++;
        return (DBObject)(c.dbget());
    }
    
    class MyInputStream extends InputStream {
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
            
            if ( _toGo <= 0 ){
                for ( DoneHook h : _hooks )
                    h.done();
                _hooks.clear();
                return -1;
            }

            int val = _in.read();
            _toGo--;
            return val;
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

    final DBCollection _collection;
    final InputStream _raw;
    final MyInputStream _user;
    final BSONDecoder _decoder;
    
    final int _len;
    final int _id;
    final int _responseTo;
    final int _operation;
    
    final int _flags;
    final long _cursor;
    final int _startingFrom;
    final int _num;

    private DBObject _peek;
    private int _readSoFar;
    private List<DoneHook> _hooks = new LinkedList<DoneHook>();
    
    static interface DoneHook {
        void done();
        void error( IOException ioe );
    }
    
    static ThreadLocal<BSONDecoder> TL = new ThreadLocal<BSONDecoder>(){
        protected BSONDecoder initialValue(){
            return new BSONDecoder();
        }
    };
}
