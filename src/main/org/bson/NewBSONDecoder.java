/**
 * Copyright (C) 2012 10gen Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bson;

import org.bson.io.Bits;
import org.bson.types.ObjectId;
import static org.bson.BSON.*;

// Java
import java.io.IOException;
import java.io.InputStream;
import java.io.DataInputStream;
import java.io.UnsupportedEncodingException;

/**
 * A new implementation of the bson decoder.
 *
 * @deprecated This class is NOT a part of public API and will be dropped in 3.x versions.
 */
@Deprecated
public class NewBSONDecoder implements BSONDecoder {

    @Override
    public BSONObject readObject(final byte [] pData) {
        _length = pData.length;
        final BasicBSONCallback c = new BasicBSONCallback();
        decode(pData, c);
        return (BSONObject)c.get();
   }

    @Override
    public BSONObject readObject(final InputStream pIn) throws IOException {
        // Slurp in the data and convert to a byte array.
        _length = Bits.readInt(pIn);

        if (_data == null || _data.length < _length) {
            _data = new byte[_length];
        }

        (new DataInputStream(pIn)).readFully(_data, 4, (_length - 4));

        return readObject(_data);
    }

    @Override
    public int decode(final byte [] pData, final BSONCallback pCallback) {
        _data = pData;
        _pos = 4;
        _callback = pCallback;
        _decode();
        return _length;
    }

    @Override
    public int decode(final InputStream pIn, final BSONCallback pCallback) throws IOException {
        _length = Bits.readInt(pIn);

        if (_data == null || _data.length < _length) {
            _data = new byte[_length];
        }

        (new DataInputStream(pIn)).readFully(_data, 4, (_length - 4));

        return decode(_data, pCallback);
    }

    private final void _decode() {
        _callback.objectStart();
        while (decodeElement());
        _callback.objectDone();
    }

    private final String readCstr() {
        int length = 0;
        final int offset = _pos;

        while (_data[_pos++] != 0) length++;

        try {
            return new String(_data, offset, length, DEFAULT_ENCODING);
        } catch (final UnsupportedEncodingException uee) {
            return new String(_data, offset, length);
        }
    }

    private final String readUtf8Str() {
        final int length = Bits.readInt(_data, _pos);
        _pos += 4;

        if (length <= 0 || length > MAX_STRING) throw new BSONException("String invalid - corruption");

        try {
            final String str = new String(_data, _pos, (length - 1), DEFAULT_ENCODING);
            _pos += length;
            return str;

        } catch (final UnsupportedEncodingException uee) {
            throw new BSONException("What is in the db", uee);
        }
    }

    private final Object _readBasicObject() {
        _pos += 4;

        final BSONCallback save = _callback;
        final BSONCallback _basic = _callback.createBSONCallback();
        _callback = _basic;
        _basic.reset();
        _basic.objectStart(false);

        while( decodeElement() );
        _callback = save;
        return _basic.get();
    }

    private final void _binary(final String pName) {

        final int totalLen = Bits.readInt(_data, _pos);
        _pos += 4;

        final byte bType = _data[_pos];
        _pos += 1;

        switch ( bType ){
            case B_GENERAL: {
                    final byte [] data = new byte[totalLen];

                    System.arraycopy(_data, _pos, data, 0, totalLen);
                    _pos += totalLen;

                    _callback.gotBinary(pName, bType, data);
                    return;
            }

            case B_BINARY: {
                final int len = Bits.readInt(_data, _pos);
                _pos += 4;

                if ( len + 4 != totalLen )
                    throw new IllegalArgumentException( "bad data size subtype 2 len: " + len + " totalLen: " + totalLen );

                final byte [] data = new byte[len];
                System.arraycopy(_data, _pos, data, 0, len);
                _pos += len;
                _callback.gotBinary(pName, bType, data);
                return;
            }

            case B_UUID: {
                if ( totalLen != 16 )
                    throw new IllegalArgumentException( "bad data size subtype 3 len: " + totalLen + " != 16");

                final long part1 = Bits.readLong(_data, _pos);
                _pos += 8;

                final long part2 = Bits.readLong(_data, _pos);
                _pos += 8;

                _callback.gotUUID(pName, part1, part2);
                return;
            }
        }

        final byte [] data = new byte[totalLen];
        System.arraycopy(_data, _pos, data, 0, totalLen);
        _pos += totalLen;

        _callback.gotBinary(pName, bType, data);
    }

    private final boolean decodeElement() {

        final byte type = _data[_pos];
        _pos += 1;

        if (type == EOO) return false;

        final String name = readCstr();

        switch (type) {
            case NULL: { _callback.gotNull(name); return true; }

            case UNDEFINED: { _callback.gotUndefined(name); return true; }

            case BOOLEAN: { _callback.gotBoolean(name, (_data[_pos] > 0)); _pos += 1; return true; }

            case NUMBER: { _callback.gotDouble(name, Double.longBitsToDouble(Bits.readLong(_data, _pos))); _pos += 8; return true; }

            case NUMBER_INT: { _callback.gotInt(name, Bits.readInt(_data, _pos)); _pos += 4; return true; }

            case NUMBER_LONG: {
                _callback.gotLong(name, Bits.readLong(_data, _pos));
                _pos += 8;
                return true;
            }

            case SYMBOL: { _callback.gotSymbol(name, readUtf8Str()); return true; }
            case STRING: {  _callback.gotString(name, readUtf8Str()); return true; }

            case OID: {
                // OID is stored as big endian

                final int p1 = Bits.readIntBE(_data, _pos);
                _pos += 4;

                final int p2 = Bits.readIntBE(_data, _pos);
                _pos += 4;

                final int p3 = Bits.readIntBE(_data, _pos);
                _pos += 4;

                _callback.gotObjectId(name , new ObjectId(p1, p2, p3));
                return true;
            }

            case REF: {
                _pos += 4;

                final String ns = readCstr();

                final int p1 = Bits.readInt(_data, _pos);
                _pos += 4;

                final int p2 = Bits.readInt(_data, _pos);
                _pos += 4;

                final int p3 = Bits.readInt(_data, _pos);
                _pos += 4;

                _callback.gotDBRef(name , ns, new ObjectId(p1, p2, p3));

                return true;
            }

            case DATE: { _callback.gotDate(name , Bits.readLong(_data, _pos)); _pos += 8; return true; }


            case REGEX: {
                _callback.gotRegex(name, readCstr(), readCstr());
                return true;
            }

            case BINARY: { _binary(name); return true; }

            case CODE: { _callback.gotCode(name, readUtf8Str()); return true; }

            case CODE_W_SCOPE: {
                _pos += 4;
                _callback.gotCodeWScope(name, readUtf8Str(), _readBasicObject());
                return true;
            }

            case ARRAY:
               _pos += 4;
                _callback.arrayStart(name);
                while (decodeElement());
                _callback.arrayDone();
                return true;

            case OBJECT:
                _pos += 4;
                _callback.objectStart(name);
                while (decodeElement());
                _callback.objectDone();
                return true;

            case TIMESTAMP:
                int i = Bits.readInt(_data, _pos);
                _pos += 4;

                int time = Bits.readInt(_data, _pos);
                _pos += 4;

                _callback.gotTimestamp(name, time, i);
                return true;

            case MINKEY: _callback.gotMinKey(name); return true;
            case MAXKEY: _callback.gotMaxKey(name); return true;

            default: throw new UnsupportedOperationException( "BSONDecoder doesn't understand type : " + type + " name: " + name  );
        }
    }

    private static final int MAX_STRING = ( 32 * 1024 * 1024 );
    private static final String DEFAULT_ENCODING = "UTF-8";

    private byte [] _data;
    private int _length;
    private int _pos = 0;
    private BSONCallback _callback;
}

