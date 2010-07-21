// BSONDecoder.java

package org.bson;

import static org.bson.BSON.*;

import java.io.*;

import org.bson.io.*;
import org.bson.types.*;

public class BSONDecoder {
    
    public BSONObject readObject( byte[] b ){
        try {
            return readObject( new ByteArrayInputStream( b ) );
        }
        catch ( IOException ioe ){
            throw new RuntimeException( "should be impossible" , ioe );
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
            return decode( new Input( new ByteArrayInputStream(b) ) , callback );
        }
        catch ( IOException ioe ){
            throw new RuntimeException( "should be impossible" , ioe );
        }
    }


    public int decode( InputStream in , BSONCallback callback )
        throws IOException {
        return decode( new Input( in ) , callback );
    }
    
    public int decode( Input in  , BSONCallback callback )
        throws IOException {

        if ( _in != null || _callback != null )
            throw new IllegalStateException( "not ready" );
        
        _in = in;
        _callback = callback;
        
        try {
            return decode();
        }
        finally {
            _in = null;
            _callback = null;
        }
    }
    
    int decode()
        throws IOException {

        final int start = _in._read;
        
        final int len = _in.readInt();

        _callback.objectStart();
        while ( decodeElement() );
        _callback.objectDone();
        
        final int read = _in._read - start;

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
            _callback.gotString( name , _in.readUTF8String() );
            break;

        case OID:
            _callback.gotObjectId( name , new ObjectId( _in.readInt() , _in.readInt() , _in.readInt() ) );
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

    void _binary( String name )
        throws IOException {
        final int totalLen = _in.readInt();
        final byte bType = _in.read();
        
        switch ( bType ){
        case B_GENERAL: {
            final byte[] data = new byte[totalLen];
            _in.fill( data );
            _callback.gotBinaryArray( name , data );
            return;
        }
        case B_BINARY: {
            final int len = _in.readInt();
            if ( len + 4 != totalLen )
                throw new IllegalArgumentException( "bad data size subtype 2 len: " + len + " totalLen: " + totalLen );
            
            final byte[] data = new byte[len];
            _in.fill( data );
            _callback.gotBinaryArray( name , data );
            return;
        }
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
    
    BSONObject _readBasicObject()
        throws IOException {
        _in.readInt();
        
        BSONCallback save = _callback;
        BSONCallback _basic = _callback.createBSONCallback();
        _callback = _basic;
        _basic.reset();
        _basic.objectStart(false);

        while( decodeElement() );
        _callback = save;
        return (BSONObject)(_basic.get());
    }
    
    class Input {
        Input( InputStream in ){
            _in = in;
            _read = 0;
        }
        
        int readInt()
            throws IOException {
            _read += 4;
            return Bits.readInt( _in );
        }

        long readLong()
            throws IOException {
            _read += 8;
            return Bits.readLong( _in );
        }

        double readDouble()
            throws IOException {
            return Double.longBitsToDouble( readLong() );
        }

        byte read()
            throws IOException {
            _read++;
            return (byte)(_in.read() & 0xFF);
        }

        void fill( byte b[] )
            throws IOException {
            fill( b , b.length );
        }

        void fill( byte b[] , int len )
            throws IOException {
            int off = 0;
            while ( len > 0 ){
                int x = _in.read( b , off , len );
                _read += x;
                off += x;
                len -= x;
            }
        }

        String readCStr()
            throws IOException {
            
            _stringBuffer.reset();
            
            while ( true ){
                byte b = read();
                if ( b == 0 )
                    break;
                _stringBuffer.write( b );
            }
            
            String out = null;
            try {
                out = _stringBuffer.asString( "UTF-8" );
            }
            catch ( UnsupportedOperationException e ){
                throw new RuntimeException( "impossible" , e );
            }
            _stringBuffer.reset();
            return out;
        }

        String readUTF8String()
            throws IOException {
            int size = readInt();
            if ( size < 0 || size > ( 3 * 1024 * 1024 ) )
                throw new RuntimeException( "bad string size: " + size );
            byte[] b = size < _random.length ? _random : new byte[size];

            fill( b , size );
            
            try {
                return new String( b , 0 , size - 1 , "UTF-8" );
            }
            catch ( java.io.UnsupportedEncodingException uee ){
                throw new RuntimeException( "impossible" , uee );
            }
        }
        
        int _read;
        final InputStream _in;
    }


    private Input _in;
    private BSONCallback _callback;
    private byte[] _random = new byte[1024];

    private PoolOutputBuffer _stringBuffer = new PoolOutputBuffer();
}
