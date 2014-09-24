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

package com.mongodb.connection;

import com.mongodb.ServerAddress;

/**
 * A description of a connection to a MongoDB server.
 *
 * @since 3.0
 */
public class ConnectionDescription {
    private final ServerAddress serverAddress;
    private final ServerVersion serverVersion;
    private final ServerType serverType;
    private final int maxBatchCount;
    private final int maxDocumentSize;
    private final int maxMessageSize;

    /**
     * Construct an instance.
     *
     * @param serverAddress   the server address
     * @param serverVersion   the server version
     * @param serverType      the server type
     * @param maxBatchCount   the max batch count
     * @param maxDocumentSize the max document size in bytes
     * @param maxMessageSize  the max message size in bytes
     */
    public ConnectionDescription(final ServerAddress serverAddress, final ServerVersion serverVersion, final ServerType serverType,
                                 final int maxBatchCount,
                                 final int maxDocumentSize, final int maxMessageSize) {
        this.serverAddress = serverAddress;
        this.serverType = serverType;
        this.maxBatchCount = maxBatchCount;
        this.maxDocumentSize = maxDocumentSize;
        this.maxMessageSize = maxMessageSize;
        this.serverVersion = serverVersion;
    }

    /**
     * Gets the server address.
     *
     * @return the server address
     */
    public ServerAddress getServerAddress() {
        return serverAddress;
    }

    /**
     * Gets the version of the server.
     *
     * @return the server version
     */
    public ServerVersion getServerVersion() {
        return serverVersion;
    }

    /**
     * Gets the server type.
     *
     * @return the server type
     */
    public ServerType getServerType() {
        return serverType;
    }

    /**
     * Gets the max batch count for bulk write operations.
     *
     * @return the max batch count
     */
    public int getMaxBatchCount() {
        return maxBatchCount;
    }

    /**
     * Gets the max document size in bytes for documents to be stored in collections.
     *
     * @return the max document size in bytes
     */
    public int getMaxDocumentSize() {
        return maxDocumentSize;
    }

    /**
     * Gets the max message size in bytes for wire protocol messages to be sent to the server.
     *
     * @return the max message size in bytes.
     */
    public int getMaxMessageSize() {
        return maxMessageSize;
    }
}

