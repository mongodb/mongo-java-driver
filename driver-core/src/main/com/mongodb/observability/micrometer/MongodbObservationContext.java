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

package com.mongodb.observability.micrometer;

import com.mongodb.ServerAddress;
import com.mongodb.connection.ConnectionId;
import com.mongodb.lang.Nullable;
import io.micrometer.observation.transport.Kind;
import io.micrometer.observation.transport.SenderContext;

/**
 * A MongoDB-specific {@link SenderContext} for Micrometer observations.
 * <p>
 * Extends {@link SenderContext} with {@link Kind#CLIENT} to preserve the client span kind
 * in the tracing bridge. Provides a MongoDB-specific type that users can filter on
 * when registering {@code ObservationHandler} or {@code ObservationConvention} instances.
 * </p>
 * <p>
 * Domain fields (commandName, databaseName, etc.) are populated by the driver after
 * the observation is started and before it is stopped. The {@code ObservationConvention}
 * reads these fields at stop time to produce the final tag key-values.
 * </p>
 *
 * @since 5.7
 */
public class MongodbObservationContext extends SenderContext<Object> {

    @Nullable
    private MongodbObservation observationType;
    @Nullable
    private String commandName;
    @Nullable
    private String databaseName;
    @Nullable
    private String collectionName;
    @Nullable
    private ServerAddress serverAddress;
    @Nullable
    private ConnectionId connectionId;
    @Nullable
    private Long cursorId;
    @Nullable
    private Long transactionNumber;
    @Nullable
    private String sessionId;
    @Nullable
    private String queryText;
    @Nullable
    private String responseStatusCode;
    private boolean isUnixSocket;

    /**
     * Creates a new {@code MongodbObservationContext} with {@link Kind#CLIENT} kind.
     */
    public MongodbObservationContext() {
        super((carrier, key, value) -> { }, Kind.CLIENT);
    }

    /**
     * @return the command name, or null if not set
     */
    @Nullable
    public String getCommandName() {
        return commandName;
    }

    /**
     * @param commandName the command name
     */
    public void setCommandName(@Nullable final String commandName) {
        this.commandName = commandName;
    }

    /**
     * @return the database name, or null if not set
     */
    @Nullable
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * @param databaseName the database name
     */
    public void setDatabaseName(@Nullable final String databaseName) {
        this.databaseName = databaseName;
    }

    /**
     * @return the collection name, or null if not set
     */
    @Nullable
    public String getCollectionName() {
        return collectionName;
    }

    /**
     * @param collectionName the collection name
     */
    public void setCollectionName(@Nullable final String collectionName) {
        this.collectionName = collectionName;
    }

    /**
     * @return the server address, or null if not set
     */
    @Nullable
    public ServerAddress getServerAddress() {
        return serverAddress;
    }

    /**
     * @param serverAddress the server address
     */
    public void setServerAddress(@Nullable final ServerAddress serverAddress) {
        this.serverAddress = serverAddress;
    }

    /**
     * @return the connection ID, or null if not set
     */
    @Nullable
    public ConnectionId getConnectionId() {
        return connectionId;
    }

    /**
     * @param connectionId the connection ID
     */
    public void setConnectionId(@Nullable final ConnectionId connectionId) {
        this.connectionId = connectionId;
    }

    /**
     * @return the observation type, or null if not set
     */
    @Nullable
    public MongodbObservation getObservationType() {
        return observationType;
    }

    /**
     * @param observationType the observation type
     */
    public void setObservationType(final MongodbObservation observationType) {
        this.observationType = observationType;
    }

    /**
     * @return the cursor ID, or null if not set
     */
    @Nullable
    public Long getCursorId() {
        return cursorId;
    }

    /**
     * @param cursorId the cursor ID
     */
    public void setCursorId(@Nullable final Long cursorId) {
        this.cursorId = cursorId;
    }

    /**
     * @return the transaction number, or null if not set
     */
    @Nullable
    public Long getTransactionNumber() {
        return transactionNumber;
    }

    /**
     * @param transactionNumber the transaction number
     */
    public void setTransactionNumber(@Nullable final Long transactionNumber) {
        this.transactionNumber = transactionNumber;
    }

    /**
     * @return the logical session ID, or null if not set
     */
    @Nullable
    public String getSessionId() {
        return sessionId;
    }

    /**
     * @param sessionId the logical session ID
     */
    public void setSessionId(@Nullable final String sessionId) {
        this.sessionId = sessionId;
    }

    /**
     * @return true if the connection uses a Unix domain socket
     */
    public boolean isUnixSocket() {
        return isUnixSocket;
    }

    /**
     * @param unixSocket whether the connection uses a Unix domain socket
     */
    public void setUnixSocket(final boolean unixSocket) {
        isUnixSocket = unixSocket;
    }

    /**
     * @return the query text, or null if not set
     */
    @Nullable
    public String getQueryText() {
        return queryText;
    }

    /**
     * @param queryText the query text
     */
    public void setQueryText(@Nullable final String queryText) {
        this.queryText = queryText;
    }

    /**
     * @return the response status code, or null if not set
     */
    @Nullable
    public String getResponseStatusCode() {
        return responseStatusCode;
    }

    /**
     * @param responseStatusCode the response status code
     */
    public void setResponseStatusCode(@Nullable final String responseStatusCode) {
        this.responseStatusCode = responseStatusCode;
    }
}
