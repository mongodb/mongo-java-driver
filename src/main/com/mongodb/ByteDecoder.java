// ByteDecoder.java

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

import com.mongodb.util.SimplePool;

import java.util.Date;
import java.util.regex.Pattern;
import java.nio.ByteBuffer;


/** 
 * Deserializes a string from the database into a <code>DBObject</code>.
 */
public class ByteDecoder extends Bytes {

    /** Gets a new <code>ByteDecoder</code> from the pool.
     * @param base the database
     * @param coll the collection
     * @return the new <code>ByteDecoder</code>
     */
    static protected ByteDecoder get( DBBase base , DBCollection coll ){
        ByteDecoder bd = _pool.get();
        bd.reset();
        bd._base = base;
        bd._collection = coll;
        return bd;
    }

    /** Returns this decoder to the pool.
     */
    protected void done(){
        _pool.done( this );
    }

    final static SimplePool<ByteDecoder> _pool = new SimplePool<ByteDecoder>( "ByteDecoders" , NUM_ENCODERS * 3 , NUM_ENCODERS * 6 ){

        protected ByteDecoder createNew(){
	    if ( D ) System.out.println( "creating new ByteDecoder" );
            return new ByteDecoder();
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

    /** Returns this decoder to its starting state with a new <code>ByteBuffer</code> to decode.
     * @param buf new <code>ByteBuffer</code>
     */
    public void reset( ByteBuffer buf ){
        if ( _private )
            throw new IllegalStateException( "can't reset private ByteDecoder" );

        _buf = buf;
        if ( _buf.order() != Bytes.ORDER )
            throw new IllegalArgumentException( "byte order of passed in buffer is not correct" );
    }

    void reset(){
        _buf.position( 0 );
        _buf.limit( _buf.capacity() );
        _buf.order( Bytes.ORDER );        
    }
    
    /** Decode an object.
     * @return the decoded object
     */
    public DBObject readObject(){
        if ( _buf.position() >= _buf.limit() )
            return null;

        final int start = _buf.position();
        final int len = _buf.getInt();
        
        DBObject created = _create("");
        
        while ( decodeNext( created , "" ) > 1 ) {
            // intentionally empty
        }
        
        if ( _buf.position() - start != len )
            throw new MongoInternalException( "lengths don't match " + (_buf.position() - start) + " != " + len );
        
        return created;
    }

    private DBObject _create( String path ){
        
        Class c = null;

        if ( _collection != null && _collection._objectClass != null){
            if ( path.length() == 0 ){
                c = _collection._objectClass;
            }
            else {
                c = _collection.getInternalClass( path );
            }
            
        }
        
        if ( c != null ){
            try {
                return (DBObject)c.newInstance();
            }
            catch ( InstantiationException ie ){
                ie.printStackTrace();
                throw new MongoInternalException( "can't instantiate a : " + c , ie );
            }
            catch ( IllegalAccessException iae ){
                iae.printStackTrace();
                throw new MongoInternalException( "can't instantiate a : " + c , iae );
            }
        }
        return new BasicDBObject();
    }

    /** Decodes the serialized object into the given <code>DBObject</code>.
     * @param o object to which to add fields
     * @return the number of characters decoded
     */
    protected int decodeNext( DBObject o , String path ){
        final int start = _buf.position();
        final byte type = _buf.get();

        if ( type == EOO )
            return 1;
        
        String name = readCStr();
        
        if ( path.length() == 0 ) 
            path = name;
        else
            path = path + "." + name;

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

        case NUMBER_LONG:
            o.put( name , _buf.getLong() );
            break;	    

        case SYMBOL:
            // intentional fallthrough
        case STRING:
            int size = _buf.getInt() - 1;
            _buf.get( _namebuf , 0 , size );
            try {
                o.put( name , new String( _namebuf , 0 , size , "UTF-8" ) );
            }
            catch ( java.io.UnsupportedEncodingException uee ){
                throw new MongoInternalException( "impossible" , uee );
            }
            _buf.get(); // skip over length
            break;

        case OID:
            o.put( name , new ObjectId( _buf.getLong() , _buf.getInt() ) );
            break;
            
        case REF:
            _buf.getInt();  // length of ctring that follows
            String ns = readCStr();
            ObjectId theOID = new ObjectId( _buf.getLong() , _buf.getInt() );
            if ( theOID.equals( Bytes.COLLECTION_REF_ID ) )
                o.put( name , _base.getCollectionFromFull( ns ) );
            else 
                o.put( name , new DBPointer( o , name , _base , ns , theOID ) );
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
            throw new UnsupportedOperationException( "can't handle CODE yet" );

        case ARRAY:
            created = new BasicDBList();
            _buf.getInt();  // total size - we don't care....

            while (decodeNext( created , path ) > 1 ) {
                // intentionally empty
            }

            o.put( name , created );
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
            
            while (decodeNext( created , path ) > 1 ) {
                // intentionally empty
            }
            
            o.put( name , created );
            break;

        case TIMESTAMP:
            int i = _buf.getInt();
            int time = _buf.getInt();

            o.put( name, new DBTimestamp(time, i) );
            break;

        case MINKEY:
            o.put( name, "MinKey" );
            break;

        case MAXKEY:
            o.put( name, "MaxKey" );
            break;

        default:
            throw new UnsupportedOperationException( "ByteDecoder can't handle type : " + type );
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
     
        throw new UnsupportedOperationException( "can't handle binary type : " + bType );
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

    private final byte _namebuf[] = new byte[ MAX_STRING ];

    ByteBuffer _buf;
    private final boolean _private;

    DBBase _base;
    DBCollection _collection;

}

