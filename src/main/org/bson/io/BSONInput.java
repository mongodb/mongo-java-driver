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
package org.bson.io;

import org.bson.BSONException;
import org.bson.BasicBSONDecoder;

import java.io.IOException;
import java.io.InputStream;

public class BSONInput {

    public BSONInput(InputStream in){
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
        return Bits.readInt( _inputBuffer , _need(4) );
    }

    public int readIntBE()
        throws IOException {
        return Bits.readIntBE( _inputBuffer , _need(4) );
    }

    public long readLong()
        throws IOException {
        return Bits.readLong( _inputBuffer , _need(8) );
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
        int have = _len - _pos;
        int tocopy = Math.min( len , have );
        System.arraycopy( _inputBuffer , _pos , b , 0 , tocopy );

        _pos += tocopy;
        _read += tocopy;

        len -= tocopy;

        int off = tocopy;
        while ( len > 0 ){
            int x = _raw.read( b , off , len );
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

    public String readCStr()
        throws IOException {

        boolean isAscii = true;

        // short circuit 1 byte strings
        _random[0] = read();
        if (_random[0] == 0) {
            return "";
        }

        _random[1] = read();
        if (_random[1] == 0) {
            String out = ONE_BYTE_STRINGS[_random[0]];
            if (out != null) {
                return out;
            }
            return new String(_random, 0, 1, "UTF-8");
        }

        _stringBuffer.reset();
        _stringBuffer.write(_random[0]);
        _stringBuffer.write(_random[1]);

        isAscii = _isAscii(_random[0]) && _isAscii(_random[1]);

        while ( true ){
            byte b = read();
            if ( b == 0 )
                break;
            _stringBuffer.write( b );
            isAscii = isAscii && _isAscii( b );
        }

        String out = null;
        if ( isAscii ){
            out = _stringBuffer.asAscii();
        }
        else {
            try {
                out = _stringBuffer.asString( "UTF-8" );
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
        int size = readInt();
        // this is just protection in case it's corrupted, to avoid huge strings
        if ( size <= 0 || size > ( 32 * 1024 * 1024 ) )
            throw new BSONException( "bad string size: " + size );

        if ( size < _inputBuffer.length / 2 ){
            if ( size == 1 ){
                read();
                return "";
            }

            return new String( _inputBuffer , _need(size) , size - 1 , "UTF-8" );
        }

        byte[] b = size < _random.length ? _random : new byte[size];

        fill( b , size );

        try {
            return new String( b , 0 , size - 1 , "UTF-8" );
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

    private byte[] _random = new byte[1024]; // has to be used within a single function
    private byte[] _inputBuffer = new byte[1024];
    private PoolOutputBuffer _stringBuffer = new PoolOutputBuffer();

    protected int _pos; // current offset into _inputBuffer
    protected int _len; // length of valid data in _inputBuffer


    protected int _max = 4; // max number of total bytes allowed to ready

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
