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
import io.micrometer.common.KeyValue;
import io.micrometer.common.KeyValues;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationConvention;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;

/**
 * Default {@link ObservationConvention} for MongoDB observations.
 * <p>
 * Reads domain fields from {@link MongodbObservationContext} and produces the standard MongoDB
 * low-cardinality and high-cardinality key-values. Users can provide a custom convention via
 * {@link MicrometerObservabilitySettings.Builder#observationConvention(ObservationConvention)}.
 * </p>
 *
 * @since 5.7
 */
public class DefaultMongodbObservationConvention implements ObservationConvention<MongodbObservationContext> {

    @Override
    public boolean supportsContext(final Observation.Context context) {
        return context instanceof MongodbObservationContext;
    }

    @Override
    public KeyValues getLowCardinalityKeyValues(final MongodbObservationContext context) {
        MongodbObservation observationType = context.getObservationType();
        if (observationType == null) {
            return KeyValues.empty();
        } else if (observationType == MongodbObservation.MONGODB_TRANSACTION) {
            return KeyValues.of(MongodbObservation.TransactionLowCardinalityKeyNames.SYSTEM.withValue("mongodb"));
        } else if (observationType == MongodbObservation.MONGODB_OPERATION) {
            return getOperationLowCardinalityKeyValues(context);
        } else {
            return getCommandLowCardinalityKeyValues(context);
        }
    }

    @Override
    public KeyValues getHighCardinalityKeyValues(final MongodbObservationContext context) {
        // High-cardinality values are only read by a recording span: skip the string formatting when the
        // sampler dropped the span. Low-cardinality values are still produced because metrics handlers on the
        // same registry consume them.
        if (context.getObservationType() == MongodbObservation.MONGODB_COMMAND && context.isRecording()) {
            return getCommandHighCardinalityKeyValues(context);
        }
        return KeyValues.empty();
    }

    private KeyValues getOperationLowCardinalityKeyValues(final MongodbObservationContext context) {
        String commandName = context.getCommandName();
        String databaseName = context.getDatabaseName();
        String collectionName = context.getCollectionName();

        // Built as one array: each KeyValues.and() merges and re-sorts, which dominated the per-span cost.
        KeyValue[] values = new KeyValue[5];
        int n = 0;
        values[n++] = MongodbObservation.OperationLowCardinalityKeyNames.SYSTEM.withValue("mongodb");
        if (databaseName != null) {
            values[n++] = MongodbObservation.OperationLowCardinalityKeyNames.NAMESPACE.withValue(databaseName);
        }
        if (collectionName != null) {
            values[n++] = MongodbObservation.OperationLowCardinalityKeyNames.COLLECTION.withValue(collectionName);
        }
        if (commandName != null) {
            values[n++] = MongodbObservation.OperationLowCardinalityKeyNames.OPERATION_NAME.withValue(commandName);
            values[n++] = MongodbObservation.OperationLowCardinalityKeyNames.OPERATION_SUMMARY.withValue(
                    summary(commandName, databaseName, collectionName));
        }
        return KeyValues.of(Arrays.copyOf(values, n));
    }

    private KeyValues getCommandLowCardinalityKeyValues(final MongodbObservationContext context) {
        String commandName = context.getCommandName();
        String databaseName = context.getDatabaseName();
        String collectionName = context.getCollectionName();
        String cmdName = commandName != null ? commandName : "";
        String dbName = databaseName != null ? databaseName : "";

        KeyValue[] values = new KeyValue[9];
        int n = 0;
        values[n++] = MongodbObservation.CommandLowCardinalityKeyNames.SYSTEM.withValue("mongodb");
        values[n++] = MongodbObservation.CommandLowCardinalityKeyNames.NAMESPACE.withValue(dbName);
        values[n++] = MongodbObservation.CommandLowCardinalityKeyNames.QUERY_SUMMARY.withValue(
                summary(cmdName, dbName, collectionName));
        values[n++] = MongodbObservation.CommandLowCardinalityKeyNames.COMMAND_NAME.withValue(cmdName);
        if (collectionName != null) {
            values[n++] = MongodbObservation.CommandLowCardinalityKeyNames.COLLECTION.withValue(collectionName);
        }
        ServerAddress serverAddress = context.getServerAddress();
        if (serverAddress != null) {
            values[n++] = MongodbObservation.CommandLowCardinalityKeyNames.SERVER_ADDRESS.withValue(serverAddress.getHost());
            values[n++] = MongodbObservation.CommandLowCardinalityKeyNames.SERVER_PORT.withValue(
                    String.valueOf(serverAddress.getPort()));
            values[n++] = MongodbObservation.CommandLowCardinalityKeyNames.NETWORK_TRANSPORT.withValue(
                    context.isUnixSocket() ? "unix" : "tcp");
        }
        String responseStatusCode = context.getResponseStatusCode();
        if (responseStatusCode != null) {
            values[n++] = MongodbObservation.CommandLowCardinalityKeyNames.RESPONSE_STATUS_CODE.withValue(responseStatusCode);
        }
        return KeyValues.of(Arrays.copyOf(values, n));
    }

    private static String summary(final String commandName, @Nullable final String databaseName,
            @Nullable final String collectionName) {
        String dbName = databaseName != null ? databaseName : "";
        return collectionName != null
                ? commandName + " " + dbName + "." + collectionName
                : commandName + " " + dbName;
    }

    private KeyValues getCommandHighCardinalityKeyValues(final MongodbObservationContext context) {
        KeyValue[] values = new KeyValue[9];
        int n = 0;
        String queryText = context.getQueryText();
        if (queryText != null) {
            values[n++] = MongodbObservation.HighCardinalityKeyNames.QUERY_TEXT.withValue(queryText);
        }
        ConnectionId connectionId = context.getConnectionId();
        if (connectionId != null) {
            values[n++] = MongodbObservation.HighCardinalityKeyNames.CLIENT_CONNECTION_ID.withValue(
                    String.valueOf(connectionId.getLocalValue()));
            values[n++] = MongodbObservation.HighCardinalityKeyNames.SERVER_CONNECTION_ID.withValue(
                    String.valueOf(connectionId.getServerValue()));
        }
        Long cursorId = context.getCursorId();
        if (cursorId != null) {
            values[n++] = MongodbObservation.HighCardinalityKeyNames.CURSOR_ID.withValue(String.valueOf(cursorId));
        }
        Long transactionNumber = context.getTransactionNumber();
        if (transactionNumber != null) {
            values[n++] = MongodbObservation.HighCardinalityKeyNames.TRANSACTION_NUMBER.withValue(
                    String.valueOf(transactionNumber));
        }
        String sessionId = context.getSessionId();
        if (sessionId != null) {
            values[n++] = MongodbObservation.HighCardinalityKeyNames.SESSION_ID.withValue(sessionId);
        }
        // Exception tags from observation error
        Throwable error = context.getError();
        if (error != null) {
            values[n++] = MongodbObservation.HighCardinalityKeyNames.EXCEPTION_MESSAGE.withValue(String.valueOf(error.getMessage()));
            values[n++] = MongodbObservation.HighCardinalityKeyNames.EXCEPTION_TYPE.withValue(error.getClass().getName());
            values[n++] = MongodbObservation.HighCardinalityKeyNames.EXCEPTION_STACKTRACE.withValue(getStackTraceAsString(error));
        }
        return n == 0 ? KeyValues.empty() : KeyValues.of(Arrays.copyOf(values, n));
    }

    private static String getStackTraceAsString(final Throwable throwable) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        throwable.printStackTrace(pw);
        return sw.toString();
    }
}
