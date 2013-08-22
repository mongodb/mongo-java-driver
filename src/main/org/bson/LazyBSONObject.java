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

import org.bson.io.BSONByteBuffer;
import org.bson.types.*;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 * @author antoine
 * @author brendan
 * @author scotthernandez
 * @author Kilroy Wuz Here
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


    class ElementRecord {
        ElementRecord( final String name, final int offset ){
            this.name = name;
            this.offset = offset;
            this.type = getElementType( offset - 1 );
            this.fieldNameSize = sizeCString( offset );
            this.valueOffset = offset + fieldNameSize;
        }

        final String name;
        /**
         * The offset the record begins at.
         */
        final byte type;
        final int fieldNameSize;
        final int valueOffset;
        final int offset;
    }

    class LazyBSONKeyIterator implements Iterator<String> {

        public boolean hasNext(){
            return !isElementEmpty( offset );
        }

        public String next(){
            int fieldSize = sizeCString( offset + 1);
            int elementSize = getElementBSONSize( offset );
            String key = _input.getCString( offset + 1);
            offset += fieldSize + elementSize + 1;
            return key;
        }

        public void remove(){
            throw new UnsupportedOperationException( "Read only" );
        }

        int offset = _doc_start_offset + FIRST_ELMT_OFFSET;
    }

    /**
     * @deprecated This class is NOT a part of public API and will be dropped in 3.x versions.
     */
    @Deprecated
    public class LazyBSONKeySet extends ReadOnlySet<String> {

        /**
         * This method runs in time linear to the total size of all keys in the document.
         *
         * @return the number of keys in the document
         */
        @Override
        public int size(){
            int size = 0;
            Iterator<String> iter = iterator();
            while(iter.hasNext()) {
                iter.next();
                ++size;
            }
            return size;
        }

        @Override
        public boolean isEmpty(){
            return LazyBSONObject.this.isEmpty();
        }

        @Override
        public boolean contains( Object o ){
            for ( String key : this ){
                if ( key.equals( o ) ){
                    return true;
                }
            }
            return false;
        }

        @Override
        public Iterator<String> iterator(){
            return new LazyBSONKeyIterator();
        }

        @Override
        public String[] toArray(){
            String[] a = new String[size()];
            return toArray(a);
        }

        @SuppressWarnings( "unchecked" )
        @Override
        public <T> T[] toArray(T[] a) {
            int size = size();

            T[] localArray = a.length >= size ? a :
                    (T[]) java.lang.reflect.Array.newInstance(a.getClass().getComponentType(), size);

            int i = 0;
            for ( String key : this ){
                localArray[i++] = (T) key;
            }

            if (localArray.length > i) {
                localArray[i] = null;
            }
            return localArray;
        }

        @Override
        public boolean add( String e ){
            throw new UnsupportedOperationException( "Not supported yet." );
        }

        @Override
        public boolean remove( Object o ){
            throw new UnsupportedOperationException( "Not supported yet." );
        }

        @Override
        public boolean containsAll( Collection<?> collection ){
            for ( Object item : collection ){
                if ( !contains( item ) ){
                    return false;
                }
            }
            return true;
        }
    }

    class LazyBSONEntryIterator implements Iterator<Map.Entry<String, Object>> {

        public boolean hasNext(){
            return !isElementEmpty( offset );
        }

        public Map.Entry<String, Object> next(){
            int fieldSize = sizeCString(offset + 1);
            int elementSize = getElementBSONSize(offset);
            String key = _input.getCString(offset + 1);
            final ElementRecord nextElementRecord = new ElementRecord(key, ++offset);
            offset += fieldSize + elementSize;
            return new Map.Entry<String, Object>() {
                @Override
                public String getKey() {
                    return nextElementRecord.name;
                }

                @Override
                public Object getValue() {
                    return getElementValue(nextElementRecord);
                }

                @Override
                public Object setValue(Object value) {
                    throw new UnsupportedOperationException("Read only");
                }

                @Override
                public boolean equals(Object o) {
                    if (!(o instanceof Map.Entry))
                        return false;
                    Map.Entry e = (Map.Entry) o;
                    return getKey().equals(e.getKey()) && getValue().equals(e.getValue());
                }

                @Override
                public int hashCode() {
                    return getKey().hashCode() ^ getValue().hashCode();
                }

                @Override
                public String toString() {
                    return getKey() + "=" + getValue();
                }
            };
        }

        public void remove(){
            throw new UnsupportedOperationException( "Read only" );
        }

        int offset = _doc_start_offset + FIRST_ELMT_OFFSET;
    }

    class LazyBSONEntrySet extends ReadOnlySet<Map.Entry<String, Object>>  {
        @Override
        public int size() {
            return LazyBSONObject.this.keySet().size();
        }

        @Override
        public boolean isEmpty() {
            return LazyBSONObject.this.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            Iterator<Map.Entry<String, Object>> iter = iterator();
            while (iter.hasNext()) {
                if (iter.next().equals(o)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            for (Object cur : c) {
                if (!contains(cur)) {
                    return false;
                }
            }

            return true;
        }

        @Override
        public Iterator<Map.Entry<String, Object>> iterator() {
            return new LazyBSONEntryIterator();
        }

        @Override
        public Object[] toArray() {
            Map.Entry[] array = new Map.Entry[size()];
            return toArray(array);
        }

        @SuppressWarnings( "unchecked" )
        @Override
        public <T> T[] toArray(T[] a) {
            int size = size();

            T[] localArray = a.length >= size ? a :
                    (T[]) java.lang.reflect.Array.newInstance(a.getClass().getComponentType(), size);

            Iterator<Map.Entry<String, Object>> iter = iterator();
            int i = 0;
            while(iter.hasNext()) {
                localArray[i++] = (T) iter.next();
            }

            if (localArray.length > i) {
                localArray[i] = null;
            }

            return localArray;
        }
    }

    // Base class that throws UnsupportedOperationException for any method that writes to the Set
    abstract class ReadOnlySet<E> implements Set<E> {

        @Override
        public boolean add(E e) {
            throw new UnsupportedOperationException("Read-only Set");
        }

        @Override
        public boolean remove(Object o) {
            throw new UnsupportedOperationException("Read-only Set");
        }

        @Override
        public boolean addAll(Collection<? extends E> c) {
            throw new UnsupportedOperationException("Read-only Set");
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            throw new UnsupportedOperationException("Read-only Set");
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            throw new UnsupportedOperationException("Read-only Set");
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException("Read-only Set");
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
        //get element up to the key
        ElementRecord element = getElement(key);
        
        //no found if null/empty
        if (element == null) {
            return null;
        }
        
        return getElementValue(element);
        
    }
    
    /**
     * returns the ElementRecord for the given key, or null if not found
     * @param key the field/key to find
     * @return ElementRecord for key, or null
     */
    ElementRecord getElement(String key){
        int offset = _doc_start_offset + FIRST_ELMT_OFFSET;
 
        while ( !isElementEmpty( offset ) ){
            int fieldSize = sizeCString( offset + 1 );
            int elementSize = getElementBSONSize( offset );
            String name = _input.getCString( ++offset);

            if (name.equals(key)) {
                return new ElementRecord( name, offset );
            }
            offset += ( fieldSize + elementSize);
        }

        return null;
    }


    /**
     * returns all the ElementRecords in this document
     * @return list of ElementRecord
     */
    List<ElementRecord> getElements(){
        int offset = _doc_start_offset + FIRST_ELMT_OFFSET;
        ArrayList<ElementRecord> elements = new ArrayList<LazyBSONObject.ElementRecord>();

        while ( !isElementEmpty( offset ) ){
            int fieldSize = sizeCString( offset + 1 );
            int elementSize = getElementBSONSize( offset );
            String name = _input.getCString( ++offset );
            ElementRecord rec = new ElementRecord( name, offset );
            elements.add( rec );
            offset += ( fieldSize + elementSize );
        }

        return elements;
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

    /**
     *
     * @return the set of all keys in the document
     */
    public Set<String> keySet(){
        return new LazyBSONKeySet();
    }

    /**
     * This method will be more efficient than using a combination of keySet() and get(String key)
     * @return the set of entries (key, value) in the document
     */
    public Set<Map.Entry<String, Object>> entrySet(){
        return new LazyBSONEntrySet();
    }

    /**
     * @deprecated This method is NOT a part of public API and will be dropped in 3.x versions.
     */
    @Deprecated
    protected boolean isElementEmpty( int offset ){
        return getElementType( offset ) == BSON.EOO;
    }

    public boolean isEmpty(){
        return isElementEmpty( _doc_start_offset + FIRST_ELMT_OFFSET );
    }

    private int getBSONSize( final int offset ){
        return _input.getInt( offset );
    }

    public int getBSONSize(){
        return getBSONSize( _doc_start_offset );
    }
    
    public int pipe(OutputStream os) throws IOException {
        os.write(_input.array(), _doc_start_offset, getBSONSize());
        return getBSONSize();
    }

    private String getElementFieldName( final int offset ){
        return _input.getCString( offset );
    }

    /**
     * @deprecated This method is NOT a part of public API and will be dropped in 3.x versions.
     */
    @Deprecated
    protected byte getElementType( final int offset ){
        return _input.get( offset );
    }

    /**
     * @deprecated This method is NOT a part of public API and will be dropped in 3.x versions.
     */
    @Deprecated
    protected int getElementBSONSize( int offset ){
        int x = 0;
        byte type = getElementType( offset++ );
        int n = sizeCString( offset );
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
                int part1 = sizeCString( valueOffset );
                int part2 = sizeCString( valueOffset + part1 );
                x = part1 + part2;
                break;
            default:
                throw new BSONException( "Invalid type " + type + " for field " + getElementFieldName( offset ) );
        }
        return x;
    }


    /**
     * Returns the size of the BSON cstring at the given offset in the buffer
     * @param offset the offset into the buffer
     * @return the size of the BSON cstring, including the null terminator
     *
     * @deprecated This method is NOT a part of public API and will be dropped in 3.x versions.
     */
    @Deprecated
    protected int sizeCString( int offset ){
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

    /**
     * @deprecated This method is NOT a part of public API and will be dropped in 3.x versions.
     */
    @Deprecated
    protected Object getElementValue( ElementRecord record ){
        switch ( record.type ){
            case BSON.EOO:
            case BSON.UNDEFINED:
            case BSON.NULL:
                return null;
            case BSON.MAXKEY:
                return new MaxKey();
            case BSON.MINKEY:
                return new MinKey();
            case BSON.BOOLEAN:
                return ( _input.get( record.valueOffset ) != 0 );
            case BSON.NUMBER_INT:
                return _input.getInt( record.valueOffset );
            case BSON.TIMESTAMP:
                int inc = _input.getInt( record.valueOffset );
                int time = _input.getInt( record.valueOffset + 4 );
                return new BSONTimestamp( time, inc );
            case BSON.DATE:
                return new Date( _input.getLong( record.valueOffset ) );
            case BSON.NUMBER_LONG:
                return _input.getLong( record.valueOffset );
            case BSON.NUMBER:
                return Double.longBitsToDouble( _input.getLong( record.valueOffset ) );
            case BSON.OID:
                return new ObjectId( _input.getIntBE( record.valueOffset ),
                                     _input.getIntBE( record.valueOffset + 4 ),
                                     _input.getIntBE( record.valueOffset + 8 ) );
            case BSON.SYMBOL:
                return new Symbol( _input.getUTF8String( record.valueOffset ) );
            case BSON.CODE:
                return new Code( _input.getUTF8String( record.valueOffset ) );
            case BSON.STRING:
                return _input.getUTF8String( record.valueOffset );
            case BSON.CODE_W_SCOPE:
                int strsize = _input.getInt( record.valueOffset + 4 );
                String code = _input.getUTF8String( record.valueOffset + 4 );
                BSONObject scope =
                        (BSONObject) _callback.createObject( _input.array(), record.valueOffset + 4 + 4 + strsize );
                return new CodeWScope( code, scope );
            case BSON.REF:
                int csize = _input.getInt( record.valueOffset );
                String ns = _input.getCString( record.valueOffset + 4 );
                int oidOffset = record.valueOffset + csize + 4;
                ObjectId oid = new ObjectId( _input.getIntBE( oidOffset ),
                                             _input.getIntBE( oidOffset + 4 ),
                                             _input.getIntBE( oidOffset + 8 ) );
                return _callback.createDBRef( ns, oid );
            case BSON.OBJECT:
                return _callback.createObject( _input.array(), record.valueOffset );
            case BSON.ARRAY:
                return _callback.createArray( _input.array(), record.valueOffset );
            case BSON.BINARY:
                return readBinary( record.valueOffset );
            case BSON.REGEX:
                int patternCStringSize = sizeCString( record.valueOffset );
                String pattern = _input.getCString( record.valueOffset );
                String flags = _input.getCString( record.valueOffset + patternCStringSize + 1 );
                return Pattern.compile( pattern, BSON.regexFlags( flags ) );
            default:
                throw new BSONException(
                        "Invalid type " + record.type + " for field " + getElementFieldName( record.offset ) );
        }
    }

    private Object readBinary( int valueOffset ){
        final int totalLen = _input.getInt( valueOffset );
        valueOffset += 4;
        final byte bType = _input.get( valueOffset );
        valueOffset += 1;

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

    protected int getOffset(){
        return _doc_start_offset;
    }

    protected byte[] getBytes() {
        return _input.array();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LazyBSONObject that = (LazyBSONObject) o;

        return Arrays.equals(this._input.array(), that._input.array());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(_input.array());
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

    /**
     * @deprecated Please use {@link #getOffset()} instead.
     */
    @Deprecated
    protected final int _doc_start_offset;

    /**
     *  @deprecated Please use {@link #getBytes()} to access underlying bytes.
     */
    @Deprecated
    protected final BSONByteBuffer _input; // TODO - Guard this with synchronicity?
    // callback is kept to create sub-objects on the fly

    /**
     * @deprecated This field is NOT a part of public API and will be dropped in 3.x versions.
     */
    @Deprecated
    protected final LazyBSONCallback _callback;
    private static final Logger log = Logger.getLogger( "org.bson.LazyBSONObject" );
}
