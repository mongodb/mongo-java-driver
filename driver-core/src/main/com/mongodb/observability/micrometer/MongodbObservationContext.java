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
import com.mongodb.annotations.Beta;
import com.mongodb.annotations.Reason;
import com.mongodb.connection.ConnectionId;
import com.mongodb.internal.observability.micrometer.MongodbObservation;
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
@Beta(Reason.CLIENT)
public class MongodbObservationContext extends SenderContext<Object> {

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

    public MongodbObservationContext() {
        super((carrier, key, value) -> { }, Kind.CLIENT);
    }

    @Nullable
    public String getCommandName() {
        return commandName;
    }

    public void setCommandName(@Nullable final String commandName) {
        this.commandName = commandName;
    }

    @Nullable
    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(@Nullable final String databaseName) {
        this.databaseName = databaseName;
    }

    @Nullable
    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(@Nullable final String collectionName) {
        this.collectionName = collectionName;
    }

    @Nullable
    public ServerAddress getServerAddress() {
        return serverAddress;
    }

    public void setServerAddress(@Nullable final ServerAddress serverAddress) {
        this.serverAddress = serverAddress;
    }

    @Nullable
    public ConnectionId getConnectionId() {
        return connectionId;
    }

    public void setConnectionId(@Nullable final ConnectionId connectionId) {
        this.connectionId = connectionId;
    }

    @Nullable
    public MongodbObservation getObservationType() {
        return observationType;
    }

    public void setObservationType(final MongodbObservation observationType) {
        this.observationType = observationType;
    }

    @Nullable
    public Long getCursorId() {
        return cursorId;
    }

    public void setCursorId(@Nullable final Long cursorId) {
        this.cursorId = cursorId;
    }

    @Nullable
    public Long getTransactionNumber() {
        return transactionNumber;
    }

    public void setTransactionNumber(@Nullable final Long transactionNumber) {
        this.transactionNumber = transactionNumber;
    }

    @Nullable
    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(@Nullable final String sessionId) {
        this.sessionId = sessionId;
    }

    public boolean isUnixSocket() {
        return isUnixSocket;
    }

    public void setUnixSocket(final boolean unixSocket) {
        isUnixSocket = unixSocket;
    }

    @Nullable
    public String getQueryText() {
        return queryText;
    }

    public void setQueryText(@Nullable final String queryText) {
        this.queryText = queryText;
    }

    @Nullable
    public String getResponseStatusCode() {
        return responseStatusCode;
    }

    public void setResponseStatusCode(@Nullable final String responseStatusCode) {
        this.responseStatusCode = responseStatusCode;
    }
}
