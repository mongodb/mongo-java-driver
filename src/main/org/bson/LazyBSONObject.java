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

import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import org.bson.io.Bits;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.CodeWScope;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;

/**
 *
 * @author antoine
 */
public class LazyBSONObject implements BSONObject {

    final static int FIRST_ELMT_OFFSET = 4;

    public class LazyBSONIterator implements Iterator<String> {

        public boolean hasNext() {
            return !isElementEmpty(offset);
        }

        public String next() {
            String key = getElementFieldName(offset);
            offset += getElementBSONSize(offset);
            return key;
        }

        public void remove() {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        int offset = FIRST_ELMT_OFFSET;
    }

    public class LazyBSONKeySet implements Set<String> {

        public int size() {
            int size = 0;
            for (String key : this) {
                ++size;
            }
            return size;
        }

        public boolean isEmpty() {
            return LazyBSONObject.this.isEmpty();
        }

        public boolean contains(Object o) {
            for (String key : this) {
                if (key.equals(o)) {
                    return true;
                }
            }
            return false;
        }

        public Iterator<String> iterator() {
            return new LazyBSONIterator();
        }

        public Object[] toArray() {
            String[] array = new String[size()];
            return toArray(array);
        }

        @SuppressWarnings("unchecked")
        public <T> T[] toArray(T[] ts) {
            int i = 0;
            for (String key : this) {
                ts[++i] = (T) key;
            }
            return ts;
        }

        public boolean add(String e) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        public boolean remove(Object o) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        public boolean containsAll(Collection<?> clctn) {
            for (Object item : clctn) {
                if (!contains(item)) {
                    return false;
                }
            }
            return true;
        }

        public boolean addAll(Collection<? extends String> clctn) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        public boolean retainAll(Collection<?> clctn) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        public boolean removeAll(Collection<?> clctn) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        public void clear() {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    }

    public LazyBSONObject(byte[] data, LazyBSONCallback cbk) {
        this(data, 0, cbk);
    }

    public LazyBSONObject(byte[] data, int offset, LazyBSONCallback cbk) {
        _data = data;
        _start = offset;
        _cbk = cbk;
    }

    public Object put(String key, Object v) {
        throw new UnsupportedOperationException("Object is read only");
    }

    public void putAll(BSONObject o) {
        throw new UnsupportedOperationException("Object is read only");
    }

    public void putAll(Map m) {
        throw new UnsupportedOperationException("Object is read only");
    }

    public Object get(String key) {
        int offset = FIRST_ELMT_OFFSET;
        boolean found = false;
        while (!isElementEmpty(offset)) {
            String name = getElementFieldName(offset);
            if (name.equals(key)) {
                found = true;
                break;
            }
            offset += getElementBSONSize(offset);
        }
        if (!found)
            return null;
        
        return getElementValue(offset);
    }

    public Map toMap() {
        throw new UnsupportedOperationException("Not Supported");
    }

    public Object removeField(String key) {
        throw new UnsupportedOperationException("Object is read only");
    }

    @Deprecated
    public boolean containsKey(String s) {
        return containsField(s);
    }

    public boolean containsField(String s) {
        return keySet().contains(s);
    }

    public Set<String> keySet() {
        return new LazyBSONKeySet();
    }

    private boolean isElementEmpty(int elmtOff) {
        return getElementType(elmtOff) == BSON.EOO;
    }

    public boolean isEmpty() {
        return isElementEmpty(FIRST_ELMT_OFFSET);
    }

    private int getBSONSize(int objOff) {
        return Bits.readInt(_data, objOff);
    }

    public int getBSONSize() {
        return getBSONSize(_start);
    }

    private String getElementFieldName(int elmtOff) {
//        return readElementCStringValueAscii(_start + elmtOff + 1);
        return readElementCStringValue(_start + elmtOff + 1);
    }

    private int getElementFieldNameSize(int elmtOff) {
        int offset = _start + elmtOff + 1;
        int end = offset;
        while (_data[end] != 0) {
            ++end;
        }
        return end - offset + 1;
    }

    private byte getElementType(int elmtOff) {
        return _data[_start + elmtOff];
    }

    private int getElementBSONSize(int elmtOff) {
        int x = 0;
        byte type = getElementType(elmtOff);
        int fieldSize = getElementFieldNameSize(elmtOff);
        int valueOffset = _start + elmtOff + 1 + fieldSize;

        switch (type) {
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
                x = Bits.readInt(_data, valueOffset) + 4;
                break;
            case BSON.CODE_W_SCOPE:
                x = getBSONSize(valueOffset);
                break;
            case BSON.REF:
                x = Bits.readInt(_data, valueOffset) + 4 + 12;
                break;
            case BSON.OBJECT:
            case BSON.ARRAY:
                x = getBSONSize(valueOffset);
                break;
            case BSON.BINARY:
                x = Bits.readInt(_data, valueOffset) + 4 + 1/*subtype*/;
                break;
            case BSON.REGEX:
                // 2 cstrs
                int end = valueOffset;
                while (_data[end] != 0) {
                    ++end;
                }
                ++end;
                while (_data[end] != 0) {
                    ++end;
                }
                ++end;
                x = end - valueOffset;
                break;
            default:
                throw new BSONException("Invalid type " + type + " for field " + getElementFieldName(elmtOff));
        }
        return x + fieldSize + 1;
    }
    
    private String readElementStringValue(int valueOffset) {
        int size = Bits.readInt(_data, valueOffset);
        return new String(_data, valueOffset + 4, size);
    }
    
    private String readElementStringValueAscii(int valueOffset) {
        int size = Bits.readInt(_data, valueOffset);
        char[] chars = new char[size];
        valueOffset += 4;
        for (int i = 0; i < size; ++i) {
            chars[i] = (char) _data[valueOffset + i];
        }
        return new String(chars, 0, size);
    }
    
    private String readElementCStringValue(int valueOffset) {
        int end = valueOffset;
        while (_data[end] != 0) {
            ++end;
        }
        int len = end - valueOffset;
        if (len == 3 && _data[valueOffset] == '_' && _data[valueOffset + 1] == 'i' && _data[valueOffset + 2] == 'd')
            return "_id";
        return new String(_data, valueOffset, len);
    }
    
    private String readElementCStringValueAscii(int valueOffset) {
        int end = valueOffset;
        while (_data[end] != 0) {
            ++end;
        }
        int len = end - valueOffset;
        char[] chars = new char[len];
        for (int i = 0; i < len; ++i) {
            chars[i] = (char) _data[valueOffset + i];
        }
        return new String(chars, 0, len);
    }
    
    private Object getElementValue(int elmtOff) {
        int x = 0;
        byte type = getElementType(elmtOff);
        int fieldSize = getElementFieldNameSize(elmtOff);
        int valueOffset = _start + elmtOff + 1 + fieldSize;

        switch (type) {
            case BSON.EOO:
            case BSON.UNDEFINED:
            case BSON.NULL:
                return null;
            case BSON.MAXKEY:
                return new MaxKey();
            case BSON.MINKEY:
                return new MinKey();
            case BSON.BOOLEAN:
                return (_data[valueOffset] != 0);
            case BSON.NUMBER_INT:
                return Bits.readInt(_data, valueOffset);
            case BSON.TIMESTAMP:
                int inc = Bits.readInt(_data, valueOffset);
                int time = Bits.readInt(_data, valueOffset + 4);
                return new BSONTimestamp(time, inc);
            case BSON.DATE:
                return new Date(Bits.readLong(_data, valueOffset));
            case BSON.NUMBER_LONG:
                return Bits.readLong(_data, valueOffset);
            case BSON.NUMBER:
                return Double.longBitsToDouble(Bits.readLong(_data, valueOffset));
            case BSON.OID:
                return new ObjectId(Bits.readIntBE(_data, valueOffset) , 
                        Bits.readIntBE(_data, valueOffset + 4) , 
                        Bits.readIntBE(_data, valueOffset + 8) );
            case BSON.SYMBOL:
                return new Symbol(readElementStringValue(valueOffset));
            case BSON.CODE:
                return new Code(readElementStringValue(valueOffset));
            case BSON.STRING:
                return readElementStringValue(valueOffset);
            case BSON.CODE_W_SCOPE:
                int size = Bits.readInt(_data, valueOffset);
                int strsize = Bits.readInt(_data, valueOffset + 4);
                String code = readElementStringValue(valueOffset + 4);
                BSONObject scope = (BSONObject) _cbk.createObject(_data, valueOffset + 4 + 4 + strsize);
                return new CodeWScope(code, scope);
            case BSON.REF:
                int csize = Bits.readInt(_data, valueOffset);
                String ns = readElementCStringValue(valueOffset + 4);
                int oidOffset = valueOffset + csize + 4;
                ObjectId oid = new ObjectId(Bits.readIntBE(_data, oidOffset) , 
                        Bits.readIntBE(_data, oidOffset + 4) , 
                        Bits.readIntBE(_data, oidOffset + 8) );
                return new BasicBSONObject( "$ns" , ns ).append( "$id" , oid );
            case BSON.OBJECT:
                return _cbk.createObject(_data, valueOffset);
            case BSON.ARRAY:
                return _cbk.createObject(_data, valueOffset);
            case BSON.BINARY:
                return readBinary(valueOffset);
            case BSON.REGEX:
                String pattern = readElementCStringValue(valueOffset);
                // calculate offset, string length is not reliable
                int end = valueOffset;
                while (_data[end] != 0) {
                    ++end;
                }
                ++end;
                String flags = readElementCStringValue(end);
                return Pattern.compile( pattern , BSON.regexFlags( flags ) );
            default:
                throw new BSONException("Invalid type " + type + " for field " + getElementFieldName(elmtOff));
        }
    }

    protected Object readBinary( int valueOffset ) {
        final int totalLen = Bits.readInt(_data, valueOffset);
        valueOffset += 4;
        final byte bType = _data[valueOffset];
        valueOffset++;

        byte[] bin;
        switch ( bType ){
        case BSON.B_GENERAL: {
            bin = new byte[totalLen];
            System.arraycopy(_data, valueOffset, bin, 0, bin.length);
            return bin;
        }
        case BSON.B_BINARY:
            final int len = Bits.readInt(_data, valueOffset);
            valueOffset += 4;
            bin = new byte[len];
            System.arraycopy(_data, valueOffset, bin, 0, bin.length);
            return bin;
        case BSON.B_UUID:
            if ( totalLen != 16 )
                throw new IllegalArgumentException( "bad data size subtype 3 len: " + totalLen + " != 16");

            long part1 = Bits.readLong(_data, valueOffset);
            valueOffset += 8;
            long part2 = Bits.readLong(_data, valueOffset);
            return new UUID(part1, part2);
        }

        bin = new byte[totalLen];
        System.arraycopy(_data, valueOffset, bin, 0, bin.length);
        return new Binary(bType, bin);
    }

    /** Returns a JSON serialization of this object
     * @return JSON serialization
     */
    public String toString(){
        return com.mongodb.util.JSON.serialize( this );
    }

    private byte[] _data;
    private int _start;
    // callback is kept to create sub-objects on the fly
    private LazyBSONCallback _cbk;
}
