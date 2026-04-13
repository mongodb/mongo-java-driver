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

package com.mongodb.internal.observability.micrometer;

import io.micrometer.common.KeyValues;
import io.micrometer.observation.GlobalObservationConvention;
import io.micrometer.observation.Observation;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Default {@link ObservationConvention} for MongoDB observations.
 * <p>
 * Reads domain fields from {@link MongodbContext} and produces the standard MongoDB
 * low-cardinality and high-cardinality key-values. Users can override this by registering
 * a {@code GlobalObservationConvention<MongodbContext>} on their {@code ObservationRegistry}.
 * </p>
 *
 * @since 5.7
 */
public class DefaultMongodbObservationConvention implements GlobalObservationConvention<MongodbContext> {

    @Override
    public boolean supportsContext(final Observation.Context context) {
        return context instanceof MongodbContext;
    }

    @Override
    public KeyValues getLowCardinalityKeyValues(final MongodbContext context) {
        if (context.getObservationType() == MongodbObservation.MONGODB_OPERATION) {
            return getOperationLowCardinalityKeyValues(context);
        } else {
            return getCommandLowCardinalityKeyValues(context);
        }
    }

    @Override
    public KeyValues getHighCardinalityKeyValues(final MongodbContext context) {
        if (context.getObservationType() == MongodbObservation.MONGODB_COMMAND) {
            return getCommandHighCardinalityKeyValues(context);
        }
        return KeyValues.empty();
    }

    private KeyValues getOperationLowCardinalityKeyValues(final MongodbContext context) {
        String commandName = context.getCommandName();
        String databaseName = context.getDatabaseName();
        String collectionName = context.getCollectionName();

        KeyValues kv = KeyValues.of(
                MongodbObservation.OperationLowCardinalityKeyNames.SYSTEM.withValue("mongodb"));

        if (databaseName != null) {
            kv = kv.and(MongodbObservation.OperationLowCardinalityKeyNames.NAMESPACE.withValue(databaseName));
        }
        if (collectionName != null) {
            kv = kv.and(MongodbObservation.OperationLowCardinalityKeyNames.COLLECTION.withValue(collectionName));
        }
        if (commandName != null) {
            String dbName = databaseName != null ? databaseName : "";
            String summary = commandName + " " + dbName
                    + (collectionName != null ? "." + collectionName : "");
            kv = kv.and(
                    MongodbObservation.OperationLowCardinalityKeyNames.OPERATION_NAME.withValue(commandName),
                    MongodbObservation.OperationLowCardinalityKeyNames.OPERATION_SUMMARY.withValue(summary));
        }
        return kv;
    }

    private KeyValues getCommandLowCardinalityKeyValues(final MongodbContext context) {
        String commandName = context.getCommandName();
        String databaseName = context.getDatabaseName();
        String collectionName = context.getCollectionName();
        String cmdName = commandName != null ? commandName : "";
        String dbName = databaseName != null ? databaseName : "";
        String summary = cmdName + " " + dbName
                + (collectionName != null ? "." + collectionName : "");

        KeyValues kv = KeyValues.of(
                MongodbObservation.CommandLowCardinalityKeyNames.SYSTEM.withValue("mongodb"),
                MongodbObservation.CommandLowCardinalityKeyNames.NAMESPACE.withValue(dbName),
                MongodbObservation.CommandLowCardinalityKeyNames.QUERY_SUMMARY.withValue(summary),
                MongodbObservation.CommandLowCardinalityKeyNames.COMMAND_NAME.withValue(cmdName));
        if (collectionName != null) {
            kv = kv.and(MongodbObservation.CommandLowCardinalityKeyNames.COLLECTION.withValue(collectionName));
        }
        com.mongodb.ServerAddress serverAddress = context.getServerAddress();
        if (serverAddress != null) {
            kv = kv.and(
                    MongodbObservation.CommandLowCardinalityKeyNames.SERVER_ADDRESS.withValue(serverAddress.getHost()),
                    MongodbObservation.CommandLowCardinalityKeyNames.SERVER_PORT.withValue(
                            String.valueOf(serverAddress.getPort())),
                    MongodbObservation.CommandLowCardinalityKeyNames.NETWORK_TRANSPORT.withValue(
                            context.isUnixSocket() ? "unix" : "tcp"));
        }
        String responseStatusCode = context.getResponseStatusCode();
        if (responseStatusCode != null) {
            kv = kv.and(MongodbObservation.CommandLowCardinalityKeyNames.RESPONSE_STATUS_CODE.withValue(responseStatusCode));
        }
        return kv;
    }

    private KeyValues getCommandHighCardinalityKeyValues(final MongodbContext context) {
        KeyValues kv = KeyValues.empty();

        String queryText = context.getQueryText();
        if (queryText != null) {
            kv = kv.and(MongodbObservation.HighCardinalityKeyNames.QUERY_TEXT.withValue(queryText));
        }
        com.mongodb.connection.ConnectionId connectionId = context.getConnectionId();
        if (connectionId != null) {
            kv = kv.and(
                    MongodbObservation.HighCardinalityKeyNames.CLIENT_CONNECTION_ID.withValue(
                            String.valueOf(connectionId.getLocalValue())),
                    MongodbObservation.HighCardinalityKeyNames.SERVER_CONNECTION_ID.withValue(
                            String.valueOf(connectionId.getServerValue())));
        }
        Long cursorId = context.getCursorId();
        if (cursorId != null) {
            kv = kv.and(MongodbObservation.HighCardinalityKeyNames.CURSOR_ID.withValue(
                    String.valueOf(cursorId)));
        }
        Long transactionNumber = context.getTransactionNumber();
        if (transactionNumber != null) {
            kv = kv.and(MongodbObservation.HighCardinalityKeyNames.TRANSACTION_NUMBER.withValue(
                    String.valueOf(transactionNumber)));
        }
        String sessionId = context.getSessionId();
        if (sessionId != null) {
            kv = kv.and(MongodbObservation.HighCardinalityKeyNames.SESSION_ID.withValue(sessionId));
        }

        // Exception tags from observation error
        Throwable error = context.getError();
        if (error != null) {
            kv = kv.and(
                    MongodbObservation.HighCardinalityKeyNames.EXCEPTION_MESSAGE.withValue(error.getMessage()),
                    MongodbObservation.HighCardinalityKeyNames.EXCEPTION_TYPE.withValue(error.getClass().getName()),
                    MongodbObservation.HighCardinalityKeyNames.EXCEPTION_STACKTRACE.withValue(getStackTraceAsString(error)));
        }

        return kv;
    }

    private static String getStackTraceAsString(final Throwable throwable) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        throwable.printStackTrace(pw);
        return sw.toString();
    }
}
