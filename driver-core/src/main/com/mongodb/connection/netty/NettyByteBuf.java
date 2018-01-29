/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.connection.netty;

import org.bson.ByteBuf;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

final class NettyByteBuf implements ByteBuf {

    private io.netty.buffer.ByteBuf proxied;
    private boolean isWriting = true;

    @SuppressWarnings("deprecation")
    NettyByteBuf(final io.netty.buffer.ByteBuf proxied) {
        this.proxied = proxied.order(ByteOrder.LITTLE_ENDIAN);
    }

    NettyByteBuf(final io.netty.buffer.ByteBuf proxied, final boolean isWriting) {
        this(proxied);
        this.isWriting = isWriting;
    }

    io.netty.buffer.ByteBuf asByteBuf() {
        return proxied;
    }

    @Override
    public int capacity() {
        return proxied.capacity();
    }

    @Override
    public ByteBuf put(final int index, final byte b) {
        proxied.setByte(index, b);
        return this;
    }

    @Override
    public int remaining() {
        if (isWriting) {
            return proxied.writableBytes();
        } else {
            return proxied.readableBytes();
        }
    }

    @Override
    public ByteBuf put(final byte[] src, final int offset, final int length) {
        proxied.writeBytes(src, offset, length);
        return this;
    }

    @Override
    public boolean hasRemaining() {
        return remaining() > 0;
    }

    @Override
    public ByteBuf put(final byte b) {
        proxied.writeByte(b);
        return this;
    }

    @Override
    public ByteBuf flip() {
        isWriting = !isWriting;
        return this;
    }

    @Override
    public byte[] array() {
        return proxied.array();
    }

    @Override
    public int limit() {
        if (isWriting) {
            return proxied.writerIndex() + remaining();
        } else {
            return proxied.readerIndex() + remaining();
        }
    }

    @Override
    public ByteBuf position(final int newPosition) {
        if (isWriting) {
            proxied.writerIndex(newPosition);
        } else {
            proxied.readerIndex(newPosition);
        }
        return this;
    }

    @Override
    public ByteBuf clear() {
        proxied.clear();
        return this;
    }

    @Override
    @SuppressWarnings("deprecation")
    public ByteBuf order(final ByteOrder byteOrder) {
        proxied = proxied.order(byteOrder);
        return this;
    }

    @Override
    public byte get() {
        return proxied.readByte();
    }

    @Override
    public byte get(final int index) {
        return proxied.getByte(index);
    }

    @Override
    public ByteBuf get(final byte[] bytes) {
        proxied.readBytes(bytes);
        return this;
    }

    @Override
    public ByteBuf get(final int index, final byte[] bytes) {
        proxied.getBytes(index, bytes);
        return this;
    }

    @Override
    public ByteBuf get(final byte[] bytes, final int offset, final int length) {
        proxied.readBytes(bytes, offset, length);
        return this;
    }

    @Override
    public ByteBuf get(final int index, final byte[] bytes, final int offset, final int length) {
        proxied.getBytes(index, bytes, offset, length);
        return this;
    }

    @Override
    public long getLong() {
        return proxied.readLong();
    }

    @Override
    public long getLong(final int index) {
        return proxied.getLong(index);
    }

    @Override
    public double getDouble() {
        return proxied.readDouble();
    }

    @Override
    public double getDouble(final int index) {
        return proxied.getDouble(index);
    }

    @Override
    public int getInt() {
        return proxied.readInt();
    }

    @Override
    public int getInt(final int index) {
        return proxied.getInt(index);
    }

    @Override
    public int position() {
        if (isWriting) {
            return proxied.writerIndex();
        } else {
            return proxied.readerIndex();
        }
    }

    @Override
    public ByteBuf limit(final int newLimit) {
        if (isWriting) {
            throw new UnsupportedOperationException("Can not set the limit while writing");
        } else {
            proxied.writerIndex(newLimit);
        }
        return this;
    }

    @Override
    public ByteBuf asReadOnly() {
        return this;  // TODO: do we need this method really?  Netty ByteBuf does not have this concept
    }

    @Override
    public ByteBuf duplicate() {
        return new NettyByteBuf(proxied.duplicate().retain(), isWriting);
    }

    @Override
    public ByteBuffer asNIO() {
        if (isWriting) {
            return proxied.nioBuffer(proxied.writerIndex(), proxied.writableBytes());
        } else {
            return proxied.nioBuffer(proxied.readerIndex(), proxied.readableBytes());
        }

    }

    @Override
    public int getReferenceCount() {
        return proxied.refCnt();
    }

    @Override
    public ByteBuf retain() {
        proxied.retain();
        return this;
    }

    @Override
    public void release() {
        proxied.release();
    }
}
