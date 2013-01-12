/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// BasicOutputBuffer.java

package org.bson.io;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class BasicOutputBuffer extends OutputBuffer {

    @Override
    public void write(final byte[] b) {
        write(b, 0, b.length);
    }

    @Override
    public void write(final byte[] b, final int off, final int len) {
        _ensure(len);
        System.arraycopy(b, off, _buffer, _cur, len);
        _cur += len;
        _size = Math.max(_cur, _size);
    }

    @Override
    public void write(final int b) {
        _ensure(1);
        _buffer[_cur++] = (byte) (0xFF & b);
        _size = Math.max(_cur, _size);
    }

    @Override
    public void writeInt(final int pos, final int x) {
        final int save = getPosition();
        setPosition(pos);
        writeInt(x);
        setPosition(save);
    }

    @Override
    public void backpatchSize(final int size) {
        writeInt(getPosition() - size, size);
    }

    @Override
    public int getPosition() {
        return _cur;
    }

    /**
     * @return size of data so far
     */
    @Override
    public int size() {
        return _size;
    }

    /**
     * @return bytes written
     */
    @Override
    public int pipe(final OutputStream out)
            throws IOException {
        out.write(_buffer, 0, _size);
        return _size;
    }

    @Override
    public void pipe(final SocketChannel socketChannel) throws IOException {
        socketChannel.write(ByteBuffer.wrap(_buffer, 0, _size));
    }

    /**
     * @return bytes written
     */
    public int pipe(final DataOutput out) throws IOException {
        out.write(_buffer, 0, _size);
        return _size;
    }


    void _ensure(final int more) {
        final int need = _cur + more;
        if (need < _buffer.length) {
            return;
        }

        int newSize = _buffer.length * 2;
        if (newSize <= need) {
            newSize = need + 128;
        }

        final byte[] n = new byte[newSize];
        System.arraycopy(_buffer, 0, n, 0, _size);
        _buffer = n;
    }

    private void setPosition(final int position) {
        _cur = position;
    }

    private int _cur;
    private int _size;
    private byte[] _buffer = new byte[1024];
}
