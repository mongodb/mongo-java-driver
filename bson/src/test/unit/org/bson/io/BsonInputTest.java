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

package org.bson.io;

import org.bson.ByteBufNIO;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class BsonInputTest {

    @Test
    void defaultPipeShouldCopyBytesFromInputToOutput() {
        // given
        byte[] inputBytes = "Java!".getBytes(StandardCharsets.UTF_8);

        try (BsonInput bsonInput = new ForwardingBsonInput(
                     new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(inputBytes))));
             BasicOutputBuffer output = new BasicOutputBuffer()) {
            // when
            bsonInput.pipe(output, inputBytes.length);

            // then
            assertEquals(inputBytes.length, bsonInput.getPosition());
            assertEquals(inputBytes.length, output.getPosition());
            assertArrayEquals(inputBytes, output.toByteArray());
        }
    }

    @Test
    void defaultPipeShouldCopyPartialBytesFromInputToOutput() {
        // given
        byte[] inputBytes = "Java!".getBytes(StandardCharsets.UTF_8);

        try (BsonInput bsonInput = new ForwardingBsonInput(
                     new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(inputBytes))));
             BasicOutputBuffer output = new BasicOutputBuffer()) {
            // when
            bsonInput.pipe(output, 3);

            // then
            assertEquals(3, bsonInput.getPosition());
            assertEquals(3, output.getPosition());
            assertArrayEquals("Jav".getBytes(StandardCharsets.UTF_8), output.toByteArray());
        }
    }

    /**
     * Delegates all abstract methods but does NOT override pipe,
     * so the default implementation is exercised.
     */
    private static class ForwardingBsonInput implements BsonInput {
        private final ByteBufferBsonInput delegate;

        ForwardingBsonInput(final ByteBufferBsonInput delegate) {
            this.delegate = delegate;
        }

        @Override
        public int getPosition() {
            return delegate.getPosition();
        }

        @Override
        public byte readByte() {
            return delegate.readByte();
        }

        @Override
        public void readBytes(final byte[] bytes) {
            delegate.readBytes(bytes);
        }

        @Override
        public void readBytes(final byte[] bytes, final int offset, final int length) {
            delegate.readBytes(bytes, offset, length);
        }

        @Override
        public long readInt64() {
            return delegate.readInt64();
        }

        @Override
        public double readDouble() {
            return delegate.readDouble();
        }

        @Override
        public int readInt32() {
            return delegate.readInt32();
        }

        @Override
        public String readString() {
            return delegate.readString();
        }

        @Override
        public ObjectId readObjectId() {
            return delegate.readObjectId();
        }

        @Override
        public String readCString() {
            return delegate.readCString();
        }

        @Override
        public void skipCString() {
            delegate.skipCString();
        }

        @Override
        public void skip(final int numBytes) {
            delegate.skip(numBytes);
        }

        @Override
        public BsonInputMark getMark(final int readLimit) {
            return delegate.getMark(readLimit);
        }

        @Override
        public boolean hasRemaining() {
            return delegate.hasRemaining();
        }

        @Override
        public void close() {
            delegate.close();
        }
    }
}
