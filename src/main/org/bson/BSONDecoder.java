// BSONDecoder.java

package org.bson;

import java.io.*;

import static org.bson.BSON.*;
import org.bson.io.*;
import org.bson.types.*;

public class BSONDecoder {
    
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
            // intentional fallthrough
        case STRING:
            int size = _in.readInt();
            if ( size < 0 || size > ( 3 * 1024 * 1024 ) )
                throw new RuntimeException( "bad string size: " + size );
            byte[] b = size < _random.length ? _random : new byte[size];

            _in.fill( b , size );
            
            try {
                String s = new String( b , 0 , size - 1 , "UTF-8" );
                if ( type == SYMBOL )
                    _callback.gotSymbol( name , s );
                else 
                    _callback.gotString( name , s );
            }
            catch ( java.io.UnsupportedEncodingException uee ){
                throw new RuntimeException( "impossible" , uee );
            }

            break;

        case OID:
            _callback.gotObjectId( name , new ObjectId( _in.readInt() , _in.readInt() , _in.readInt() ) );
            break;
            
        case REF:
            _in.readInt();  // length of ctring that follows
            String ns = _in.readCStr();
            ObjectId theOID = new ObjectId( _in.readInt() , _in.readInt() , _in.readInt() );
            _callback.gotDBRef( name , ns , theOID );
            /* TODO: make sure to handle somewhere
            if ( theOID.equals( BSON.COLLECTION_REF_ID ) )
                created = _base.getCollectionFromFull( ns );
            else 
                created = new DBPointer( o , name , _base , ns , theOID );
            */
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
            throw new UnsupportedOperationException( "can't handle CODE yet" );

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
        case B_BINARY:
            final int len = _in.readInt();
            if ( len + 4 != totalLen )
                throw new IllegalArgumentException( "bad data size subtype 2 len: " + len + " totalLen: " + totalLen );
            
            final byte[] data = new byte[len];
            _in.fill( data );
            _callback.gotBinaryArray( name , data );
            return;
        }
        
        byte[] data = new byte[totalLen];
        _in.fill( data );

        _callback.gotBinary( name , bType , data );
    }

    
    class Input {
        Input( InputStream in ){
            _in = in;
            _read = 0;
        }
        
        int readInt()
            throws IOException {
            int x = 0;
            x |= ( 0xFF & _in.read() ) << 0;
            x |= ( 0xFF & _in.read() ) << 8;
            x |= ( 0xFF & _in.read() ) << 16;
            x |= ( 0xFF & _in.read() ) << 24;
            _read += 4;
            return x;
        }

        long readLong()
            throws IOException {
            long x = 0;
            x |= (long)( 0xFFL & _in.read() ) << 0;
            x |= (long)( 0xFFL & _in.read() ) << 8;
            x |= (long)( 0xFFL & _in.read() ) << 16;
            x |= (long)( 0xFFL & _in.read() ) << 24;
            x |= (long)( 0xFFL & _in.read() ) << 32;
            x |= (long)( 0xFFL & _in.read() ) << 40;
            x |= (long)( 0xFFL & _in.read() ) << 48;
            x |= (long)( 0xFFL & _in.read() ) << 56;
            _read += 8;
            return x;
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
        
        int _read;
        final InputStream _in;
    }


    private Input _in;
    private BSONCallback _callback;
    private byte[] _random = new byte[1024];

    private PoolOutputBuffer _stringBuffer = new PoolOutputBuffer();
}
