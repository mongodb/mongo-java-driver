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

package org.mongodb.connection;

import org.bson.ByteBuf;

import java.io.IOException;
import java.util.List;

/**
 *
 * A full duplex stream of bytes.
 *
 * @since 3.0
 */
public interface Stream {

    /**
     * Write each buffer in the list to the stream in order, blocking until all are completely written.
     *
     * @param buffers the buffers to write
     * @throws IOException
     */
    void write(final List<ByteBuf> buffers) throws IOException;

    /**
     * Read from the stream into the given buffer, blocking until it is full.
     *
     * @param buffer the buffer to read into
     * @throws IOException
     */
    void read(final ByteBuf buffer) throws IOException;

    /**
     * The address that this stream is connected to.
     *
     * @return the address
     */
    ServerAddress getAddress();

    /**
     * Closes the connection.
     */
    void close();

    /**
     * Returns the closed state of the connection
     *
     * @return true if connection is closed
     */
    boolean isClosed();
}
