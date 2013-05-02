/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.bson;

import com.mongodb.MongoInternalException;
import org.bson.codecs.BSONObjectCodec;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.OutputBuffer;
import org.mongodb.codecs.PrimitiveCodecs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * This is meant to be pooled or cached
 * There is some per instance memory for string conversion, etc...
 */
public class BasicBSONEncoder implements BSONEncoder {

    private OutputBuffer outputBuffer;

    @Override
    public byte[] encode(final BSONObject o) {
        final OutputBuffer outputBuffer = new BasicOutputBuffer();
        set(outputBuffer);
        putObject(o);
        done();
        return outputBuffer.toByteArray();
    }

    /**
     * Encodes a {@code BSONObject}.
     * This is for the higher level api calls.
     *
     * @param document the document to encode
     * @return the number of characters in the encoding
     */
    @Override
    public int putObject(final BSONObject document) {
        try {
            new BSONObjectCodec(PrimitiveCodecs.createDefault()).encode(new BSONBinaryWriter(outputBuffer), document);
            final BufferExposingByteArrayOutputStream stream = new BufferExposingByteArrayOutputStream();
            outputBuffer.pipe(stream);
            return stream.getInternalBytes().length;
        } catch (IOException e) {
            // impossible with a byte array output stream
            throw new MongoInternalException("Received considered as impossible IOException from ByteArrayOutputStream", e);
        }
    }

    @Override
    public void done() {
        this.outputBuffer = null;
    }

    @Override
    public void set(final OutputBuffer out) {
        if (this.outputBuffer != null) {
            throw new IllegalStateException("Performing another operation at this moment");
        }
        this.outputBuffer = out;
    }


    // Just so we don't have to copy the buffer
    static class BufferExposingByteArrayOutputStream extends ByteArrayOutputStream {
        BufferExposingByteArrayOutputStream() {
            super(512);
        }

        byte[] getInternalBytes() {
            return buf;
        }
    }
}
