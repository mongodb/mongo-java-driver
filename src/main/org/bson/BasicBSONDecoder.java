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

    protected void _binary( final String name )
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

            final byte [] data = new byte[len];
            _in.fill( data );
            _callback.gotBinary( name , bType , data );
            return;
        case B_UUID:
            if ( totalLen != 16 )
                throw new IllegalArgumentException( "bad data size subtype 3 len: " + totalLen + " != 16");

            final long part1 = _in.readLong();
            final long part2 = _in.readLong();
            _callback.gotUUID(name, part1, part2);
            return;
        }

        final byte [] data = new byte[totalLen];
        _in.fill( data );

        _callback.gotBinary( name , bType , data );
    }

    Object _readBasicObject()
        throws IOException {
        _in.readInt();

        final BSONCallback save = _callback;
        final BSONCallback _basic = _callback.createBSONCallback();
        _callback = _basic;
        _basic.reset();
        _basic.objectStart(false);

        while( decodeElement() );
        _callback = save;
        return _basic.get();
    }

    protected class BSONInput {

        public BSONInput(final InputStream in){
            _raw = in;
            _read = 0;

            _pos = 0;
            _len = 0;
        }

        /**
         * ensure that there are num bytes to read
         * _pos is where to start reading from
         * @return where to start reading from
         */
        protected int _need( final int num )
                throws IOException {

            //System.out.println( "p: " + _pos + " l: " + _len + " want: " + num );

            if ( _len - _pos >= num ){
                final int ret = _pos;
                _pos += num;
                _read += num;
                return ret;
            }

            if ( num >= _inputBuffer.length )
                throw new IllegalArgumentException( "you can't need that much" );

            final int remaining = _len - _pos;
            if ( _pos > 0 ){
                System.arraycopy( _inputBuffer , _pos , _inputBuffer , 0  , remaining );

                _pos = 0;
                _len = remaining;
            }

            // read as much as possible into buffer
            int maxToRead = Math.min( _max - _read - remaining , _inputBuffer.length - _len );
            while ( maxToRead > 0 ){
                int x = _raw.read( _inputBuffer , _len ,  maxToRead);
                if ( x <= 0 )
                    throw new IOException( "unexpected EOF" );
                maxToRead -= x;
                _len += x;
            }

            int ret = _pos;
            _pos += num;
            _read += num;
            return ret;
        }

        public int readInt()
                throws IOException {
            return org.bson.io.Bits.readInt( _inputBuffer , _need(4) );
        }

        public int readIntBE()
                throws IOException {
            return org.bson.io.Bits.readIntBE( _inputBuffer , _need(4) );
        }

        public long readLong()
                throws IOException {
            return org.bson.io.Bits.readLong( _inputBuffer , _need(8) );
        }

        public double readDouble()
                throws IOException {
            return Double.longBitsToDouble( readLong() );
        }

        public byte read()
                throws IOException {
            if ( _pos < _len ){
                ++_read;
                return _inputBuffer[_pos++];
            }
            return _inputBuffer[_need(1)];
        }

        public void fill( byte b[] )
                throws IOException {
            fill( b , b.length );
        }

        public void fill( byte b[] , int len )
                throws IOException {
            // first use what we have
            final int have = _len - _pos;
            final int tocopy = Math.min( len , have );
            System.arraycopy( _inputBuffer , _pos , b , 0 , tocopy );

            _pos += tocopy;
            _read += tocopy;

            len -= tocopy;

            int off = tocopy;
            while ( len > 0 ){
                final int x = _raw.read( b , off , len );
                if (x <= 0)
                    throw new IOException( "unexpected EOF" );
                _read += x;
                off += x;
                len -= x;
            }
        }

        protected boolean _isAscii( byte b ){
            return b >=0 && b <= 127;
        }

        public String readCStr() throws IOException {

            boolean isAscii = true;

            // short circuit 1 byte strings
            _random[0] = read();
            if (_random[0] == 0) {
                return "";
            }

            _random[1] = read();
            if (_random[1] == 0) {
                final String out = ONE_BYTE_STRINGS[_random[0]];
                return (out != null) ? out : new String(_random, 0, 1, DEFAULT_ENCODING);
            }

            _stringBuffer.reset();
            _stringBuffer.write(_random, 0, 2);

            isAscii = _isAscii(_random[0]) && _isAscii(_random[1]);

            byte b;
            while ((b = read()) != 0) {
                _stringBuffer.write( b );
                isAscii = isAscii && _isAscii( b );
            }

            String out = null;
            if ( isAscii ){
                out = _stringBuffer.asAscii();
            }
            else {
                try {
                    out = _stringBuffer.asString( DEFAULT_ENCODING );
                }
                catch ( UnsupportedOperationException e ){
                    throw new BSONException( "impossible" , e );
                }
            }
            _stringBuffer.reset();
            return out;
        }

        public String readUTF8String()
                throws IOException {
            final int size = readInt();
            // this is just protection in case it's corrupted, to avoid huge strings
            if ( size <= 0 || size > MAX_STRING )
                throw new BSONException( "bad string size: " + size );

            if ( size < _inputBuffer.length / 2 ){
                if ( size == 1 ){
                    read();
                    return "";
                }

                return new String( _inputBuffer , _need(size) , size - 1 , DEFAULT_ENCODING );
            }

            final byte [] b = size < _random.length ? _random : new byte[size];

            fill( b , size );

            try {
                return new String( b , 0 , size - 1 , DEFAULT_ENCODING );
            }
            catch ( java.io.UnsupportedEncodingException uee ){
                throw new BSONException( "impossible" , uee );
            }
        }

        public int numRead() {
            return _read;
        }

        public int getPos() {
            return _pos;
        }

        public int getMax() {
            return _max;
        }

        public void setMax(int _max) {
            this._max = _max;
        }

        int _read;
        final InputStream _raw;

        int _max = 4; // max number of total bytes allowed to ready

    }

    protected BSONInput _in;
    protected BSONCallback _callback;

    private byte [] _random = new byte[1024]; // has to be used within a single function
    private byte [] _inputBuffer = new byte[1024];

    private PoolOutputBuffer _stringBuffer = new PoolOutputBuffer();

    protected int _pos; // current offset into _inputBuffer
    protected int _len; // length of valid data in _inputBuffer

    private static final int MAX_STRING = ( 32 * 1024 * 1024 );

    private static final String DEFAULT_ENCODING = "UTF-8";

    private static final boolean _isAscii( final byte b ){
        return b >=0 && b <= 127;
    }

    static final String[] ONE_BYTE_STRINGS = new String[128];
    static void _fillRange( byte min, byte max ){
        while ( min < max ){
            String s = "";
            s += (char)min;
            ONE_BYTE_STRINGS[(int)min] = s;
            min++;
        }
    }
    static {
        _fillRange( (byte)'0' , (byte)'9' );
        _fillRange( (byte)'a' , (byte)'z' );
        _fillRange( (byte)'A' , (byte)'Z' );
    }
}
