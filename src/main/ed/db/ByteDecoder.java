// ByteDecoder.java

package ed.db;

import java.util.*;
import java.util.regex.*;
import java.nio.*;
import java.nio.charset.*;

import ed.util.*;

public class ByteDecoder extends Bytes {

    static protected ByteDecoder get( DBBase base , DBCollection coll ){
        ByteDecoder bd = _pool.get();
        bd.reset();
        bd._base = base;
        bd._collection = coll;
        return bd;
    }

    protected void done(){
        _pool.done( this );
    }

    private final static int _poolSize = 6 * BUFS_PER_50M;
    private final static SimplePool<ByteDecoder> _pool = new SimplePool<ByteDecoder>( "ByteDecoders" , _poolSize , -1 ){

        protected ByteDecoder createNew(){
	    if ( D ) System.out.println( "creating new ByteDecoder" );
            return new ByteDecoder();
        }

        protected long memSize( ByteDecoder d ){
            return BUF_SIZE + ( 2 * MAX_STRING ) + 1024;
        }
    };

    // ---
    
    public ByteDecoder( ByteBuffer buf ){
        reset( buf );
        _private = false;
    }

    private ByteDecoder(){
        _buf = ByteBuffer.allocateDirect( BUF_SIZE );
        _private = true;
        reset();
    }

    public void reset( ByteBuffer buf ){
        if ( _private )
            throw new RuntimeException( "can't reset private ByteDecoder" );

        _buf = buf;
        if ( _buf.order() != Bytes.ORDER )
            throw new RuntimeException( "this is not correct" );
    }

    void reset(){
        _buf.position( 0 );
        _buf.limit( _buf.capacity() );
        _buf.order( Bytes.ORDER );        
    }
    
    public DBObject readObject(){
        if ( _buf.position() >= _buf.limit() )
            return null;

        final int start = _buf.position();
        final int len = _buf.getInt();
        
        DBObject created = null;

        if ( created == null )
            created = _create();
        
        while ( decodeNext( created ) > 1 );
        
        if ( _buf.position() - start != len )
            throw new RuntimeException( "lengths don't match " + (_buf.position() - start) + " != " + len );
        
        return created;
    }

    private DBObject _create(){
        
        final Class c = _collection == null ? null : _collection._objectClass;
        
        if ( c != null ){
            try {
                return (DBObject)c.newInstance();
            }
            catch ( InstantiationException ie ){
                throw new RuntimeException( "can't instantiate a : " + c , ie );
            }
            catch ( IllegalAccessException iae ){
                throw new RuntimeException( "can't instantiate a : " + c , iae );
            }
        }
        return new BasicDBObject();
    }

    protected int decodeNext( DBObject o ){
        final int start = _buf.position();
        final byte type = _buf.get();

        if ( type == EOO )
            return 1;
        
        String name = readCStr();

        DBObject created = null;

        switch ( type ){
        case NULL:
            o.put( name , null );
            break;

        case BOOLEAN:
            o.put( name , _buf.get() > 0 );
            break;

        case NUMBER:
            double val = _buf.getDouble();
            o.put( name , val );
            break;
	    
	case NUMBER_INT:
	    o.put( name , _buf.getInt() );
	    break;
	    
        case SYMBOL:
        case STRING:
            int size = _buf.getInt() - 1;
            _buf.get( _namebuf , 0 , size );
            try {
                o.put( name , new String( _namebuf , 0 , size , "UTF-8" ) );
            }
            catch ( java.io.UnsupportedEncodingException uee ){
                throw new RuntimeException( "impossible" , uee );
            }
            _buf.get(); // skip over length
            break;

        case OID:
            o.put( name , new ObjectId( _buf.getLong() , _buf.getInt() ) );
            break;
            
        case REF:
            int stringSize = _buf.getInt();
            String ns = readCStr();
            ObjectId theOID = new ObjectId( _buf.getLong() , _buf.getInt() );
            if ( theOID.equals( Bytes.COLLECTION_REF_ID ) )
                o.put( name , _base.getCollectionFromFull( ns ) );
            else 
                o.put( name , new DBRef( o , name , _base , ns , theOID ) );
            break;
            
        case DATE:
            o.put( name , new Date( _buf.getLong() ) );
            break;
            
        case REGEX:
            o.put( name , Pattern.compile( readCStr() , Bytes.patternFlags( readCStr() ) ) );
            break;

        case BINARY:
            o.put( name , parseBinary() );
            break;
            
        case CODE:
            throw new RuntimeException( "can't handle CODE yet" );

        case ARRAY:
            if ( created == null )
                created = new BasicDBList();
        case OBJECT:
            int embeddedSize = _buf.getInt();

            if ( created == null )
                created = _create();
            
            while ( decodeNext( created ) > 1 );
            o.put( name , created );
            break;

        default:
            throw new RuntimeException( "can't handle : " + type );
        }
        
        return _buf.position() - start;
    }
    
    byte[] parseBinary(){
        final int totalLen = _buf.getInt();
        final byte bType = _buf.get();
        
        switch ( bType ){
        case B_BINARY:
            final int len = _buf.getInt();
	    if ( D ) System.out.println( "got binary of size : " + len );
            final byte[] data = new byte[len];
            _buf.get( data );
            return data;
        }
     
        throw new RuntimeException( "can't handle binary type : " + bType );
    }

    private String readCStr(){
        int pos = 0;
        while ( true ){
            byte b = _buf.get();
            if ( b == 0 )
                break;
            _namebuf[pos++] = b;
        }
        return new String( _namebuf , 0 , pos );
    }

    int getInt(){
        return _buf.getInt();
    }

    long getLong(){
        return _buf.getLong();
    }

    boolean more(){
        return _buf.position() < _buf.limit();
    }

    long remaining(){
        return _buf.remaining();
    }

    void doneReading( int len ){
        _buf.position( len );
        _buf.flip();
    }

    private final CharsetDecoder _decoder = _utf8.newDecoder();
    private final byte _namebuf[] = new byte[ MAX_STRING ];

    ByteBuffer _buf;
    private final boolean _private;

    DBBase _base;
    DBCollection _collection;

}

