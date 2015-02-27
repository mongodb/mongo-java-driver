/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import org.bson.io.OutputBuffer;

/**
 * <p>A {@code BSONEncoder} is a class which can be used to turn documents into byte arrays. The {@code BSONEncoder} walks down through the
 * object graph and writes corresponding {@code byte} sequences into underlying {@code OutputBuffer}.</p>
 *
 * <p>This class is a part of legacy API. Please check {@link org.bson.codecs.Encoder} for a new one.</p>
 */
public interface BSONEncoder {

    /**
     * Encode a document into byte array.
     * This is a shortcut method which creates a new {@link OutputBuffer},
     * invokes the other 3 methods in a corresponding sequence:
     * <ul>
     * <li>{@link #set(org.bson.io.OutputBuffer)}</li>
     * <li>{@link #putObject(BSONObject)}</li>
     * <li>{@link #done()}</li>
     * </ul>
     * and returns the contents of the {@code OutputBuffer}.
     *
     * @param document the document to be encoded
     * @return a byte sequence
     */
    byte[] encode(BSONObject document);

    /**
     * Encoder and write a document into underlying buffer.
     *
     * @param document the document to be encoded
     * @return number of bytes written
     */
    int putObject(BSONObject document);

    /**
     * Free the resources.
     */
    void done();

    /**
     * Sets the buffer to wrich the result of encoding will be written.
     *
     * @param buffer the buffer to be used to write a byte sequences to
     */
    void set(OutputBuffer buffer);
}
