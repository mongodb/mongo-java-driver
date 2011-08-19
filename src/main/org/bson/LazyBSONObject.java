/**
 *      Copyright (C) 2008-2011 10gen Inc.
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

import org.bson.io.*;
import org.bson.types.*;

import java.util.*;
import java.util.logging.*;
import java.util.regex.*;

/**
 * @author antoine
 * @author brendan
 */
public class LazyBSONObject implements BSONObject {

    public LazyBSONObject( byte[] data, LazyBSONCallback callback ){
        this( BSONByteBuffer.wrap( data ), callback );
    }

    public LazyBSONObject( byte[] data, int offset, LazyBSONCallback callback ){
        this( BSONByteBuffer.wrap( data, offset, data.length - offset ), offset, callback );
    }

    public LazyBSONObject( BSONByteBuffer buffer, LazyBSONCallback callback ){
        this( buffer, 0, callback );
    }

    public LazyBSONObject( BSONByteBuffer buffer, int offset, LazyBSONCallback callback ){
        _callback = callback;
        _input = buffer;
        _doc_start_offset = offset;
    }

    public class LazyBSONIterator implements Iterator<String> {

        public boolean hasNext(){
            return !isElementEmpty( offset );
        }

        public String next(){
            int fieldSize = sizeCString( offset );
            int elementSize = getElementBSONSize( offset++ );
            String key = _input.getCString( offset );
            offset += ( fieldSize + elementSize );
            return key;
        }

        public void remove(){
            throw new UnsupportedOperationException( "Not supported yet." );
        }

        int offset = _doc_start_offset + FIRST_ELMT_OFFSET;
    }

    public class LazyBSONKeySet implements Set<String> {

        public int size(){
            int size = 0;
            for ( String key : this ){
                ++size;
            }
            return size;
        }

        public boolean isEmpty(){
            return LazyBSONObject.this.isEmpty();
        }

        public boolean contains( Object o ){
            for ( String key : this ){
                if ( key.equals( o ) ){
                    return true;
                }
            }
            return false;
        }

        public Iterator<String> iterator(){
            return new LazyBSONIterator();
        }

        public Object[] toArray(){
            String[] array = new String[size()];
            return toArray( array );
        }

        @SuppressWarnings( "unchecked" )
        public <T> T[] toArray( T[] ts ){
            int i = 0;
            for ( String key : this ){
                ts[++i] = (T) key;
            }
            return ts;
        }

        public boolean add( String e ){
            throw new UnsupportedOperationException( "Not supported yet." );
        }

        public boolean remove( Object o ){
            throw new UnsupportedOperationException( "Not supported yet." );
        }

        public boolean containsAll( Collection<?> clctn ){
            for ( Object item : clctn ){
                if ( !contains( item ) ){
                    return false;
                }
            }
            return true;
        }

        public boolean addAll( Collection<? extends String> clctn ){
            throw new UnsupportedOperationException( "Not supported yet." );
        }

        public boolean retainAll( Collection<?> clctn ){
            throw new UnsupportedOperationException( "Not supported yet." );
        }

        public boolean removeAll( Collection<?> clctn ){
            throw new UnsupportedOperationException( "Not supported yet." );
        }

        public void clear(){
            throw new UnsupportedOperationException( "Not supported yet." );
        }
    }

    public Object put( String key, Object v ){
        throw new UnsupportedOperationException( "Object is read only" );
    }

    public void putAll( BSONObject o ){
        throw new UnsupportedOperationException( "Object is read only" );
    }

    public void putAll( Map m ){
        throw new UnsupportedOperationException( "Object is read only" );
    }

    public Object get( String key ){
        int offset = _doc_start_offset + FIRST_ELMT_OFFSET;
        boolean found = false;
        // TODO - Memoize any key location we find
        while ( !isElementEmpty( offset ) ){
            int fieldSize = sizeCString( offset );
            int elementSize = getElementBSONSize( offset++ );
            String name = _input.getCString( offset );
            if ( name.equals( key ) ){
                found = true; // TODO - Memoize me
                break;
            }
            offset += ( fieldSize + elementSize );
        }
        if ( !found ){
            return null;
        }

        return getElementValue( offset - 1 );
    }

    public Map toMap(){
        throw new UnsupportedOperationException( "Not Supported" );
    }

    public Object removeField( String key ){
        throw new UnsupportedOperationException( "Object is read only" );
    }

    @Deprecated
    public boolean containsKey( String s ){
        return containsField( s );
    }

    public boolean containsField( String s ){
        return keySet().contains( s );
    }

    public Set<String> keySet(){
        return new LazyBSONKeySet();
    }

    private boolean isElementEmpty( int offset ){
        return getElementType( offset ) == BSON.EOO;
    }

    public boolean isEmpty(){
        return isElementEmpty( _doc_start_offset + FIRST_ELMT_OFFSET );
    }

    private int getBSONSize( final int offset ){
        return _input.getInt( offset );
    }

    public int getBSONSize(){
        return _input.getInt( _doc_start_offset );
    }

    private String getElementFieldName( final int offset ){
        return _input.getCString( offset );
    }

    private byte getElementType( final int offset ){
        return _input.get( offset );
    }

    private int getElementBSONSize( int offset ){
        int x = 0;
        byte type = getElementType( offset++ );
        int n = sizeCString( offset++ );
        int valueOffset = offset + n;
        switch ( type ){
            case BSON.EOO:
            case BSON.UNDEFINED:
            case BSON.NULL:
            case BSON.MAXKEY:
            case BSON.MINKEY:
                break;
            case BSON.BOOLEAN:
                x = 1;
                break;
            case BSON.NUMBER_INT:
                x = 4;
                break;
            case BSON.TIMESTAMP:
            case BSON.DATE:
            case BSON.NUMBER_LONG:
            case BSON.NUMBER:
                x = 8;
                break;
            case BSON.OID:
                x = 12;
                break;
            case BSON.SYMBOL:
            case BSON.CODE:
            case BSON.STRING:
                x = _input.getInt( valueOffset ) + 4;
                break;
            case BSON.CODE_W_SCOPE:
                x = _input.getInt( valueOffset );
                break;
            case BSON.REF:
                x = _input.getInt( valueOffset ) + 4 + 12;
                break;
            case BSON.OBJECT:
            case BSON.ARRAY:
                x = _input.getInt( valueOffset );
                break;
            case BSON.BINARY:
                x = _input.getInt( valueOffset ) + 4 + 1/*subtype*/;
                break;
            case BSON.REGEX:
                // 2 cstrs
                int part1 = sizeCString( valueOffset ) + 1;
                int part2 = sizeCString( valueOffset + part1 ) + 1;
                x = part1 + part2 + 4;
                break;
            default:
                throw new BSONException( "Invalid type " + type + " for field " + getElementFieldName( offset ) );
        }
        return x;
    }


    private int sizeCString( int offset ){
        offset += 1;
        int end = offset;
        while ( true ){
            byte b = _input.get( end );
            if ( b == 0 )
                break;
            else
                end++;
        }
        return end - offset + 1;
    }

    private Object getElementValue( int offset ){
        int x = 0;
        byte type = getElementType( offset );
        int fieldNameSize = sizeCString( offset++ );
        int valueOffset = offset + fieldNameSize;


        switch ( type ){
            case BSON.EOO:
            case BSON.UNDEFINED:
            case BSON.NULL:
                return null;
            case BSON.MAXKEY:
                return new MaxKey();
            case BSON.MINKEY:
                return new MinKey();
            case BSON.BOOLEAN:
                return ( _input.get( valueOffset ) != 0 );
            case BSON.NUMBER_INT:
                return _input.getInt( valueOffset );
            case BSON.TIMESTAMP:
                int inc = _input.getInt( valueOffset );
                int time = _input.getInt( valueOffset + 4 );
                return new BSONTimestamp( time, inc );
            case BSON.DATE:
                return new Date( _input.getLong( valueOffset ) );
            case BSON.NUMBER_LONG:
                return _input.getLong( valueOffset );
            case BSON.NUMBER:
                return Double.longBitsToDouble( _input.getLong( valueOffset ) );
            case BSON.OID:
                return new ObjectId( _input.getIntBE( valueOffset ),
                                     _input.getIntBE( valueOffset + 4 ),
                                     _input.getIntBE( valueOffset + 8 ) );
            case BSON.SYMBOL:
                return new Symbol( _input.getUTF8String( valueOffset ) );
            case BSON.CODE:
                return new Code( _input.getUTF8String( valueOffset ) );
            case BSON.STRING:
                return _input.getUTF8String( valueOffset );
            case BSON.CODE_W_SCOPE:
                int size = _input.getInt( valueOffset );
                int strsize = _input.getInt( valueOffset + 4 );
                String code = _input.getUTF8String( valueOffset + 4 );
                BSONObject scope = (BSONObject) _callback.createObject( _input.array(), valueOffset + 4 + 4 + strsize );
                return new CodeWScope( code, scope );
            case BSON.REF:
                int csize = _input.getInt( valueOffset );
                String ns = _input.getCString( valueOffset + 4 );
                int oidOffset = valueOffset + csize + 4;
                ObjectId oid = new ObjectId( _input.getIntBE( oidOffset ),
                                             _input.getIntBE( oidOffset + 4 ),
                                             _input.getIntBE( oidOffset + 8 ) );
                return _callback.createDBRef( ns, oid );
            case BSON.OBJECT:
                return _callback.createObject( _input.array(), valueOffset );
            case BSON.ARRAY:
                return _callback.createObject( _input.array(), valueOffset );
            case BSON.BINARY:
                return readBinary( valueOffset );
            case BSON.REGEX:
                int n = sizeCString( valueOffset );
                String pattern = _input.getCString( valueOffset );
                String flags = _input.getCString( valueOffset + n );
                return Pattern.compile( pattern, BSON.regexFlags( flags ) );
            default:
                throw new BSONException( "Invalid type " + type + " for field " + getElementFieldName( offset ) );
        }
    }

    private Object readBinary( int valueOffset ){
        final int totalLen = _input.getInt( valueOffset );
        valueOffset += 4;
        final byte bType = _input.get( valueOffset );

        byte[] bin;
        switch ( bType ){
            case BSON.B_GENERAL:{
                bin = new byte[totalLen];
                for ( int n = 0; n < totalLen; n++ ){
                    bin[n] = _input.get( valueOffset + n );
                }
                return bin;
            }
            case BSON.B_BINARY:
                final int len = _input.getInt( valueOffset );
                if ( len + 4 != totalLen )
                    throw new IllegalArgumentException(
                            "Bad Data Size; Binary Subtype 2.  { actual len: " + len + " expected totalLen: " + totalLen
                            + "}" );
                valueOffset += 4;
                bin = new byte[len];
                for ( int n = 0; n < len; n++ ){
                    bin[n] = _input.get( valueOffset + n );
                }
                return bin;
            case BSON.B_UUID:
                if ( totalLen != 16 )
                    throw new IllegalArgumentException(
                            "Bad Data Size; Binary Subtype 3 (UUID). { total length: " + totalLen + " != 16" );

                long part1 = _input.getLong( valueOffset );
                valueOffset += 8;
                long part2 = _input.getLong( valueOffset );
                return new UUID( part1, part2 );
        }

        bin = new byte[totalLen];
        for ( int n = 0; n < totalLen; n++ ){
            bin[n] = _input.get( valueOffset + n );
        }
        return bin;
    }

    /**
     * Returns a JSON serialization of this object
     *
     * @return JSON serialization
     */
    public String toString(){
        return com.mongodb.util.JSON.serialize( this );
    }

    /**
     * In a "normal" (aka not embedded) doc, this will be the offset of the first element.
     *
     * In an embedded doc because we use ByteBuffers to avoid unecessary copying the offset must be manually set in
     * _doc_start_offset
     */
    final static int FIRST_ELMT_OFFSET = 4;

    private final int _doc_start_offset;

    private final BSONByteBuffer _input; // TODO - Guard this with synchronicity?
    // callback is kept to create sub-objects on the fly
    private final LazyBSONCallback _callback;
    private static final Logger log = Logger.getLogger( "org.bson.LazyBSONObject" );
}
