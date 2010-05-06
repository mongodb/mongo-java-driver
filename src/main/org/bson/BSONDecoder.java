// BSONDecoder.java

package org.bson;

import java.io.*;

import static org.bson.BSON.*;

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
        
        int read = 0;

        final int len = _in.readInt();
        read += 4;

        _callback.objectStart();
        
        while ( true ){
            int z = decodeElement();
            read += z;
            
            if ( z <= 0 )
                throw new IllegalArgumentException( "error" );
            if ( z == 1 )
                break;
        }

        _callback.objectDone();
        if ( read != len )
            throw new IllegalArgumentException( "bad data.  lengths don't match " + read + " != " + len );
        return read;
    }

    int decodeElement()
        throws IOException {

        final byte type = _in.read();
        if ( type == EOO )
            return 1;
        
        
        
        switch ( type ){
            /*
        case NULL:
            
        case UNDEFINED:
            break;

        case BOOLEAN:
            created =_buf.get() > 0;
            break;

        case NUMBER:
            created = _buf.getDouble();
            break;
	    
        case NUMBER_INT:
            created = _buf.getInt();
            break;

        case NUMBER_LONG:
            created = _buf.getLong();
            break;	    

        case SYMBOL:
            // intentional fallthrough
        case STRING:
            int size = _buf.getInt() - 1;
            if ( size > _buf.remaining() )
                throw new MongoException( "invalid bson? size:" + size + " remaining: " + _buf.remaining() );
            _buf.get( _namebuf , 0 , size );
            try {
                created = new String( _namebuf , 0 , size , "UTF-8" );
            }
            catch ( java.io.UnsupportedEncodingException uee ){
                throw new MongoInternalException( "impossible" , uee );
            }
            _buf.get(); // skip over length
            break;

        case OID:
            created = new ObjectId( _buf.getInt() , _buf.getInt() , _buf.getInt() );
            break;
            
        case REF:
            _buf.getInt();  // length of ctring that follows
            String ns = readCStr();
            ObjectId theOID = new ObjectId( _buf.getInt() , _buf.getInt() , _buf.getInt() );
            if ( theOID.equals( Bytes.COLLECTION_REF_ID ) )
                created = _base.getCollectionFromFull( ns );
            else 
                created = new DBPointer( o , name , _base , ns , theOID );
            break;
            
        case DATE:
            created = new Date( _buf.getLong() );
            break;
            
        case REGEX:
            created = Pattern.compile( readCStr() , Bytes.regexFlags( readCStr() ) );
            break;

        case BINARY:
            created = parseBinary();
            break;
            
        case CODE:
            throw new UnsupportedOperationException( "can't handle CODE yet" );

        case ARRAY:
            created = new BasicDBList();
            _buf.getInt();  // total size - we don't care....

            while (decodeNext( (DBObject)created , path ) > 1 ) {
                // intentionally empty
            }

            break;

        case OBJECT:
            _buf.getInt();  // total size - we don't care....
            
            if ( created == null ){

                Object foo = o.get( name );
                if ( foo instanceof DBObject )
                    created = (DBObject)foo;
                
                if ( created == null )
                    created = _create( path );
            }
            
            while (decodeNext( (DBObject)created , path ) > 1 ) {
                // intentionally empty
            }
            
            DBObject theObject = (DBObject)created;
            if ( theObject.containsKey( "$ref" ) && 
                 theObject.containsKey( "$id" ) ){
                created = new DBRef( _base , theObject.get( "$ref" ).toString() , theObject.get( "$id" ) );
            }

            break;
            
        case TIMESTAMP:
            int i = _buf.getInt();
            int time = _buf.getInt();

            created = new BSONTimestamp(time, i);
            break;

        case MINKEY:
            created = "MinKey";
            break;

        case MAXKEY:
            created = "MaxKey";
            break;
            */
        default:
            throw new UnsupportedOperationException( "BSONDecoder doesn't understand type : " + type );
        }

    }
    
    class Input {
        Input( InputStream in ){
            _in = in;
        }
        
        int readInt()
            throws IOException {
            int x = 0;
            x |= ( 0xFF & _in.read() ) >> 0;
            x |= ( 0xFF & _in.read() ) >> 8;
            x |= ( 0xFF & _in.read() ) >> 16;
            x |= ( 0xFF & _in.read() ) >> 24;
            return x;
        }

        byte read()
            throws IOException {
            return (byte)(_in.read() & 0xFF);
        }

        String readCStr()
            throws IOException {
            
            throw new RuntimeException( "not done" );

        }

        final InputStream _in;
    }


    private Input _in;
    private BSONCallback _callback;
    private byte[] _random = new byte[1024];
}
