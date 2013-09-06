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

import java.util.concurrent.ExecutorService;

/**
 * A factory of streams that support both async and SSL.
 *
 * @since 3.0
 */
public class SSLNIOStreamFactory implements StreamFactory {
    private final BufferProvider bufferProvider;
    private final ExecutorService executor;

    public SSLNIOStreamFactory(final BufferProvider bufferProvider, final ExecutorService executor) {
        this.bufferProvider = bufferProvider;
        this.executor = executor;
    }

    /**
     * Create a Stream to the given address
     *
     * @param serverAddress the address
     * @return the stream
     */
    @Override
    public Stream create(final ServerAddress serverAddress) {
        return new SSLNIOStream(serverAddress, bufferProvider, executor);
    }
}
