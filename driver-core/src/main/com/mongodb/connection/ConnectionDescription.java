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

package com.mongodb.connection;

import com.mongodb.ServerAddress;
import com.mongodb.annotations.Immutable;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.connection.ServerDescription.getDefaultMaxDocumentSize;
import static com.mongodb.internal.operation.ServerVersionHelper.UNKNOWN_WIRE_VERSION;

/**
 * A description of a connection to a MongoDB server.
 *
 * @since 3.0
 */
@Immutable
public class ConnectionDescription {
    @Nullable private final ObjectId serviceId;
    private final ConnectionId connectionId;
    private final int maxWireVersion;
    private final ServerType serverType;
    private final int maxBatchCount;
    private final int maxDocumentSize;
    private final int maxMessageSize;
    private final List<String> compressors;
    private final BsonArray saslSupportedMechanisms;
    private final Integer logicalSessionTimeoutMinutes;

    private static final int DEFAULT_MAX_MESSAGE_SIZE = 0x2000000;   // 32MB
    private static final int DEFAULT_MAX_WRITE_BATCH_SIZE = 512;

    /**
     * Construct a defaulted connection description instance.
     *
     * @param serverId   the server address
     */
    public ConnectionDescription(final ServerId serverId) {
        this(new ConnectionId(serverId), UNKNOWN_WIRE_VERSION, ServerType.UNKNOWN, DEFAULT_MAX_WRITE_BATCH_SIZE,
             getDefaultMaxDocumentSize(), DEFAULT_MAX_MESSAGE_SIZE, Collections.emptyList());
    }

    /**
     * Construct an instance.
     *
     * @param connectionId    the connection id
     * @param maxWireVersion  the max wire version
     * @param serverType      the server type
     * @param maxBatchCount   the max batch count
     * @param maxDocumentSize the max document size in bytes
     * @param maxMessageSize  the max message size in bytes
     * @param compressors     the available compressors on the connection
     * @since 3.10
     */
    public ConnectionDescription(final ConnectionId connectionId, final int maxWireVersion,
                                 final ServerType serverType, final int maxBatchCount, final int maxDocumentSize,
                                 final int maxMessageSize, final List<String> compressors) {
        this(connectionId, maxWireVersion, serverType, maxBatchCount, maxDocumentSize, maxMessageSize, compressors, null);
    }

    /**
     * Construct an instance.
     *
     * @param connectionId    the connection id
     * @param maxWireVersion  the max wire version
     * @param serverType      the server type
     * @param maxBatchCount   the max batch count
     * @param maxDocumentSize the max document size in bytes
     * @param maxMessageSize  the max message size in bytes
     * @param compressors     the available compressors on the connection
     * @param saslSupportedMechanisms the supported SASL mechanisms
     * @since 4.1
     */
    public ConnectionDescription(final ConnectionId connectionId, final int maxWireVersion,
                                 final ServerType serverType, final int maxBatchCount, final int maxDocumentSize,
                                 final int maxMessageSize, final List<String> compressors,
                                 @Nullable final BsonArray saslSupportedMechanisms) {
        this(null, connectionId, maxWireVersion, serverType, maxBatchCount, maxDocumentSize, maxMessageSize, compressors,
                saslSupportedMechanisms);
    }

    /**
     * Construct an instance.
     *
     * @param connectionId    the connection id
     * @param maxWireVersion  the max wire version
     * @param serverType      the server type
     * @param maxBatchCount   the max batch count
     * @param maxDocumentSize the max document size in bytes
     * @param maxMessageSize  the max message size in bytes
     * @param compressors     the available compressors on the connection
     * @param saslSupportedMechanisms the supported SASL mechanisms
     * @param logicalSessionTimeoutMinutes the logical session timeout, in minutes
     * @since 4.10
     */
    public ConnectionDescription(final ConnectionId connectionId, final int maxWireVersion,
            final ServerType serverType, final int maxBatchCount, final int maxDocumentSize,
            final int maxMessageSize, final List<String> compressors,
            @Nullable final BsonArray saslSupportedMechanisms,
            @Nullable final Integer logicalSessionTimeoutMinutes) {
        this(null, connectionId, maxWireVersion, serverType, maxBatchCount, maxDocumentSize, maxMessageSize, compressors,
                saslSupportedMechanisms, logicalSessionTimeoutMinutes);
    }

    /**
     * Construct an instance.
     *
     * @param serviceId       the service id, which may be null
     * @param connectionId    the connection id
     * @param maxWireVersion  the max wire version
     * @param serverType      the server type
     * @param maxBatchCount   the max batch count
     * @param maxDocumentSize the max document size in bytes
     * @param maxMessageSize  the max message size in bytes
     * @param compressors     the available compressors on the connection
     * @param saslSupportedMechanisms the supported SASL mechanisms
     * @since 4.3
     */
    public ConnectionDescription(@Nullable final ObjectId serviceId, final ConnectionId connectionId, final int maxWireVersion,
                                 final ServerType serverType, final int maxBatchCount, final int maxDocumentSize,
                                 final int maxMessageSize, final List<String> compressors,
                                 @Nullable final BsonArray saslSupportedMechanisms) {
        this(serviceId, connectionId, maxWireVersion, serverType, maxBatchCount, maxDocumentSize, maxMessageSize, compressors,
                saslSupportedMechanisms, null);
    }

    private ConnectionDescription(@Nullable final ObjectId serviceId, final ConnectionId connectionId, final int maxWireVersion,
            final ServerType serverType, final int maxBatchCount, final int maxDocumentSize,
            final int maxMessageSize, final List<String> compressors,
            @Nullable final BsonArray saslSupportedMechanisms, @Nullable final Integer logicalSessionTimeoutMinutes) {
        this.serviceId = serviceId;
        this.connectionId = connectionId;
        this.serverType = serverType;
        this.maxBatchCount = maxBatchCount;
        this.maxDocumentSize = maxDocumentSize;
        this.maxMessageSize = maxMessageSize;
        this.maxWireVersion = maxWireVersion;
        this.compressors = notNull("compressors", Collections.unmodifiableList(new ArrayList<>(compressors)));
        this.saslSupportedMechanisms = saslSupportedMechanisms;
        this.logicalSessionTimeoutMinutes = logicalSessionTimeoutMinutes;
    }
    /**
     * Creates a new connection description with the set connection id
     *
     * @param connectionId the connection id
     * @return the new connection description
     * @since 3.8
     */
    public ConnectionDescription withConnectionId(final ConnectionId connectionId) {
        notNull("connectionId", connectionId);
        return new ConnectionDescription(serviceId, connectionId, maxWireVersion, serverType, maxBatchCount, maxDocumentSize,
                maxMessageSize, compressors, saslSupportedMechanisms, logicalSessionTimeoutMinutes);
    }

    /**
     * Creates a new connection description with the given service id
     *
     * @param serviceId the service id
     * @return the new connection description
     * @since 4.3
     */
    public ConnectionDescription withServiceId(final ObjectId serviceId) {
        notNull("serviceId", serviceId);
        return new ConnectionDescription(serviceId, connectionId, maxWireVersion, serverType, maxBatchCount, maxDocumentSize,
                maxMessageSize, compressors, saslSupportedMechanisms, logicalSessionTimeoutMinutes);
    }

    /**
     * Gets the server address.
     *
     * @return the server address
     */
    public ServerAddress getServerAddress() {
        return connectionId.getServerId().getAddress();
    }

    /**
     * Gets the id of the connection. If possible, this id will correlate with the connection id that the server puts in its log messages.
     *
     * @return the connection id
     */
    public ConnectionId getConnectionId() {
        return connectionId;
    }

    /**
     * Gets the id of the service this connection is to
     *
     * @return the service id, which may be null
     * @since 4.3
     */
    @Nullable
    public ObjectId getServiceId() {
        return serviceId;
    }

    /**
     * The latest version of the wire protocol that this MongoDB server is capable of using to communicate with clients.
     *
     * @return the maximum protocol version supported by this server
     * @since 3.10
     */
    public int getMaxWireVersion() {
        return maxWireVersion;
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

    /**
     * Gets the compressors supported by this connection.
     *
     * @return the non-null list of compressors supported by this connection
     */
    public List<String> getCompressors() {
        return compressors;
    }

    /**
     * Get the supported SASL mechanisms.
     *
     * @return the supported SASL mechanisms.
     * @since 4.1
     */
    @Nullable
    public BsonArray getSaslSupportedMechanisms() {
        return saslSupportedMechanisms;
    }

    /**
     * Gets the session timeout in minutes.
     *
     * @return the session timeout in minutes, or null if sessions are not supported by this connection
     * @mongodb.server.release 3.6
     * @since 4.10
     */
    @Nullable
    public Integer getLogicalSessionTimeoutMinutes() {
        return logicalSessionTimeoutMinutes;
    }
    /**
     * Get the default maximum message size.
     *
     * @return the default maximum message size.
     */
    public static int getDefaultMaxMessageSize() {
        return DEFAULT_MAX_MESSAGE_SIZE;
    }


    /**
     * Get the default maximum write batch size.
     *
     * @return the default maximum write batch size.
     */
    public static int getDefaultMaxWriteBatchSize() {
        return DEFAULT_MAX_WRITE_BATCH_SIZE;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ConnectionDescription that = (ConnectionDescription) o;

        if (maxWireVersion != that.maxWireVersion) {
            return false;
        }
        if (maxBatchCount != that.maxBatchCount) {
            return false;
        }
        if (maxDocumentSize != that.maxDocumentSize) {
            return false;
        }
        if (maxMessageSize != that.maxMessageSize) {
            return false;
        }
        if (!Objects.equals(serviceId, that.serviceId)) {
            return false;
        }
        if (!connectionId.equals(that.connectionId)) {
            return false;
        }
        if (serverType != that.serverType) {
            return false;
        }
        if (!compressors.equals(that.compressors)) {
            return false;
        }
        if (!Objects.equals(logicalSessionTimeoutMinutes, that.logicalSessionTimeoutMinutes)) {
            return false;
        }
        return Objects.equals(saslSupportedMechanisms, that.saslSupportedMechanisms);
    }

    @Override
    public int hashCode() {
        int result = connectionId.hashCode();
        result = 31 * result + maxWireVersion;
        result = 31 * result + serverType.hashCode();
        result = 31 * result + maxBatchCount;
        result = 31 * result + maxDocumentSize;
        result = 31 * result + maxMessageSize;
        result = 31 * result + compressors.hashCode();
        result = 31 * result + (serviceId != null ? serviceId.hashCode() : 0);
        result = 31 * result + (saslSupportedMechanisms != null ? saslSupportedMechanisms.hashCode() : 0);
        result = 31 * result + (logicalSessionTimeoutMinutes != null ? logicalSessionTimeoutMinutes.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ConnectionDescription{"
                + "connectionId=" + connectionId
                + ", maxWireVersion=" + maxWireVersion
                + ", serverType=" + serverType
                + ", maxBatchCount=" + maxBatchCount
                + ", maxDocumentSize=" + maxDocumentSize
                + ", maxMessageSize=" + maxMessageSize
                + ", compressors=" + compressors
                + ", logicialSessionTimeoutMinutes=" + logicalSessionTimeoutMinutes
                + ", serviceId=" + serviceId
                + '}';
    }
}

