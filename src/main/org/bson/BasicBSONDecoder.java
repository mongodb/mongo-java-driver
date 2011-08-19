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
package org.bson;

import static org.bson.BSON.*;

import java.io.*;

import org.bson.io.BSONInput;
import org.bson.io.PoolOutputBuffer;
import org.bson.types.ObjectId;


/**
 * Basic implementation of BSONDecoder interface that creates BasicBSONObject instances
 */
public class BasicBSONDecoder implements BSONDecoder {
    public BSONObject readObject( byte[] b ){
        try {
            return readObject( new ByteArrayInputStream( b ) );
        }
        catch ( IOException ioe ){
            throw new BSONException( "should be impossible" , ioe );
        }
    }

    public BSONObject readObject( InputStream in )
        throws IOException {
        BasicBSONCallback c = new BasicBSONCallback();
        decode( in , c );
        return (BSONObject)c.get();
    }

    public int decode( byte[] b , BSONCallback callback ){
        try {
            return _decode( new BSONInput( new ByteArrayInputStream(b) ) , callback );
        }
        catch ( IOException ioe ){
            throw new BSONException( "should be impossible" , ioe );
        }
    }


    public int decode( InputStream in , BSONCallback callback )
        throws IOException {
        return _decode( new BSONInput( in ) , callback );
    }
    
    private int _decode( BSONInput in  , BSONCallback callback )
        throws IOException {

        if ( _in != null || _callback != null )
            throw new IllegalStateException( "not ready" );
        
        _in = in;
        _callback = callback;
        
        if ( in.numRead() != 0 )
            throw new IllegalArgumentException( "i'm confused" );

        try {
            
            final int len = _in.readInt();
            _in.setMax(len);

            _callback.objectStart();
            while ( decodeElement() );
            _callback.objectDone();
            
            if ( _in.numRead() != len )
                throw new IllegalArgumentException( "bad data.  lengths don't match read:" + _in.numRead() + " != len:" + len );
            
            return len;
        }
        finally {
            _in = null;
            _callback = null;
        }
    }
    
    int decode( boolean first )
        throws IOException {

        final int start = _in.numRead();
        
        final int len = _in.readInt();
        if ( first )
            _in.setMax(len);

        _callback.objectStart();
        while ( decodeElement() );
        _callback.objectDone();
        
        final int read = _in.numRead() - start;

        if ( read != len ){
            //throw new IllegalArgumentException( "bad data.  lengths don't match " + read + " != " + len );
        }

        return len;
    }
    
    boolean decodeElement()
        throws IOException {

        final byte type = _in.read();
        if ( type == EOO )
            return false;
        
        String name = _in.readCStr();
        
        switch ( type ){
        case NULL:
            _callback.gotNull( name ); 
            break;
            
        case UNDEFINED:
            _callback.gotUndefined( name ); 
            break;

        case BOOLEAN:
            _callback.gotBoolean( name , _in.read() > 0 );
            break;

        case NUMBER:
            _callback.gotDouble( name , _in.readDouble() );
            break;
	    
        case NUMBER_INT:
            _callback.gotInt( name , _in.readInt() );
            break;

        case NUMBER_LONG:
            _callback.gotLong( name , _in.readLong() );
            break;	    

            
        case SYMBOL:
            _callback.gotSymbol( name , _in.readUTF8String() );
            break;
            

        case STRING:
            _callback.gotString(name, _in.readUTF8String() );
            break;

        case OID:
            // OID is stored as big endian
            _callback.gotObjectId( name , new ObjectId( _in.readIntBE() , _in.readIntBE() , _in.readIntBE() ) );
            break;
            
        case REF:
            _in.readInt();  // length of ctring that follows
            String ns = _in.readCStr();
            ObjectId theOID = new ObjectId( _in.readInt() , _in.readInt() , _in.readInt() );
            _callback.gotDBRef( name , ns , theOID );
            break;
            
        case DATE:
            _callback.gotDate( name , _in.readLong() );
            break;
            
        case REGEX:
            _callback.gotRegex( name , _in.readCStr() , _in.readCStr() );
            break;

        case BINARY:
            _binary( name );
            break;
            
        case CODE:
            _callback.gotCode( name , _in.readUTF8String() );
            break;

        case CODE_W_SCOPE:
            _in.readInt();
            _callback.gotCodeWScope( name , _in.readUTF8String() , _readBasicObject() );

            break;

        case ARRAY:
            _in.readInt();  // total size - we don't care....

            _callback.arrayStart( name );
            while ( decodeElement() );
            _callback.arrayDone();

            break;
            
            
        case OBJECT:
            _in.readInt();  // total size - we don't care....
            
            _callback.objectStart( name );
            while ( decodeElement() );
            _callback.objectDone();

            break;
            
        case TIMESTAMP:
            int i = _in.readInt();
            int time = _in.readInt();
            _callback.gotTimestamp( name , time , i );
            break;

        case MINKEY:
            _callback.gotMinKey( name );
            break;

        case MAXKEY:
            _callback.gotMaxKey( name );
            break;

        default:
            throw new UnsupportedOperationException( "BSONDecoder doesn't understand type : " + type + " name: " + name  );
        }
        
        return true;
    }

    protected void _binary( String name )
        throws IOException {
        final int totalLen = _in.readInt();
        final byte bType = _in.read();
        
        switch ( bType ){
        case B_GENERAL: {
                final byte[] data = new byte[totalLen];
                _in.fill( data );
                _callback.gotBinary( name, bType, data );
                return;
        }
        case B_BINARY:
            final int len = _in.readInt();
            if ( len + 4 != totalLen )
                throw new IllegalArgumentException( "bad data size subtype 2 len: " + len + " totalLen: " + totalLen );
            
            final byte[] data = new byte[len];
            _in.fill( data );
            _callback.gotBinary( name , bType , data );
            return;
        case B_UUID:
            if ( totalLen != 16 )
                throw new IllegalArgumentException( "bad data size subtype 3 len: " + totalLen + " != 16");
            
            long part1 = _in.readLong();
            long part2 = _in.readLong();
            _callback.gotUUID(name, part1, part2);
            return;	
        }
        
        byte[] data = new byte[totalLen];
        _in.fill( data );

        _callback.gotBinary( name , bType , data );
    }
    
    Object _readBasicObject()
        throws IOException {
        _in.readInt();
        
        BSONCallback save = _callback;
        BSONCallback _basic = _callback.createBSONCallback();
        _callback = _basic;
        _basic.reset();
        _basic.objectStart(false);

        while( decodeElement() );
        _callback = save;
        return _basic.get();
    }


    protected BSONInput _in;
    protected BSONCallback _callback;

}
