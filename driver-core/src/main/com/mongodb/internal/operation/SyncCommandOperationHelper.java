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

package com.mongodb.internal.operation;

import com.mongodb.Function;
import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.ServerApi;
import com.mongodb.internal.ClientSideOperationTimeout;
import com.mongodb.internal.binding.ConnectionSource;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.binding.WriteBinding;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import org.bson.BsonDocument;
import org.bson.FieldNameValidator;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.operation.CommandOperationHelper.addRetryableWriteErrorLabel;
import static com.mongodb.internal.operation.CommandOperationHelper.isRetryWritesEnabled;
import static com.mongodb.internal.operation.CommandOperationHelper.logRetryExecute;
import static com.mongodb.internal.operation.CommandOperationHelper.logUnableToRetry;
import static com.mongodb.internal.operation.CommandOperationHelper.noOpRetryCommandModifier;
import static com.mongodb.internal.operation.CommandOperationHelper.shouldAttemptToRetryRead;
import static com.mongodb.internal.operation.CommandOperationHelper.shouldAttemptToRetryWrite;
import static com.mongodb.internal.operation.CommandOperationHelper.transformWriteException;
import static com.mongodb.internal.operation.OperationHelper.canRetryRead;
import static com.mongodb.internal.operation.OperationHelper.canRetryWrite;
import static com.mongodb.internal.operation.SyncOperationHelper.withConnection;
import static com.mongodb.internal.operation.SyncOperationHelper.withReadConnectionSource;
import static com.mongodb.internal.operation.SyncOperationHelper.withReleasableConnection;

public final class SyncCommandOperationHelper {

    interface CommandReadTransformer<T, R> {

        /**
         * Yield an appropriate result object for the input object.
         *
         * @param t the input object
         * @return the function result
         */
        R apply(ClientSideOperationTimeout clientSideOperationTimeout, ConnectionSource source, Connection connection, T t);
    }

    interface CommandWriteTransformer<T, R> {

        /**
         * Yield an appropriate result object for the input object.
         *
         * @param t the input object
         * @return the function result
         */
        R apply(ClientSideOperationTimeout clientSideOperationTimeout, Connection connection, T t);
    }

    static class IdentityReadTransformer<T> implements CommandReadTransformer<T, T> {
        @Override
        public T apply(final ClientSideOperationTimeout clientSideOperationTimeout,
                       final ConnectionSource source, final Connection connection, final T t) {
            return t;
        }
    }

    static class IdentityWriteTransformer<T> implements CommandWriteTransformer<T, T> {
        @Override
        public T apply(final ClientSideOperationTimeout clientSideOperationTimeout, final Connection connection, final T t) {
            return t;
        }
    }

    static CommandWriteTransformer<BsonDocument, Void> writeConcernErrorTransformer() {
        return (clientSideOperationTimeout, connection, result) -> {
            WriteConcernHelper.throwOnWriteConcernError(result, connection.getDescription().getServerAddress(),
                    connection.getDescription().getMaxWireVersion());
            return null;
        };
    }

    /* Read Binding Helpers */

    static BsonDocument executeCommand(final ClientSideOperationTimeout clientSideOperationTimeout, final ReadBinding binding,
                                       final String database, final CommandCreator commandCreator, final boolean retryReads) {
        return executeCommand(clientSideOperationTimeout, binding, database, commandCreator, new BsonDocumentCodec(), retryReads);
    }

    static <T> T executeCommand(final ClientSideOperationTimeout clientSideOperationTimeout, final ReadBinding binding,
                                final String database, final CommandCreator commandCreator,
                                final CommandReadTransformer<BsonDocument, T> transformer, final boolean retryReads) {
        return executeCommand(clientSideOperationTimeout, binding, database, commandCreator, new BsonDocumentCodec(), transformer,
                retryReads);
    }

    static <T> T executeCommand(final ClientSideOperationTimeout clientSideOperationTimeout, final ReadBinding binding,
                                final String database, final CommandCreator commandCreator, final Decoder<T> decoder,
                                final boolean retryReads) {
        return executeCommand(clientSideOperationTimeout, binding, database, commandCreator, decoder, new IdentityReadTransformer<>(),
                retryReads);
    }

    static <D, T> T executeCommand(final ClientSideOperationTimeout clientSideOperationTimeout, final ReadBinding binding,
                                   final String database, final CommandCreator commandCreator, final Decoder<D> decoder,
                                   final CommandReadTransformer<D, T> transformer, final boolean retryReads) {
        return withReadConnectionSource(clientSideOperationTimeout, binding, (clientSideOperationTimeout1, source) ->
                executeCommandWithConnection(clientSideOperationTimeout1, binding, source, database, commandCreator, decoder, transformer,
                        retryReads, source.getConnection()));
    }

    static <D, T> T executeCommandWithConnection(final ClientSideOperationTimeout clientSideOperationTimeout, final ReadBinding binding,
                                                 final ConnectionSource source, final String database,
                                                 final CommandCreator commandCreator, final Decoder<D> decoder,
                                                 final CommandReadTransformer<D, T> transformer, final boolean retryReads,
                                                 final Connection connection) {
        MongoException exception;
        try {
            BsonDocument command = commandCreator.create(clientSideOperationTimeout, source.getServerDescription(),
                    connection.getDescription());
            try {
                return executeCommand(clientSideOperationTimeout, database, command, decoder, source, connection,
                        binding.getReadPreference(), transformer, binding.getSessionContext(), binding.getServerApi());
            } catch (MongoException e) {
                exception = e;

                if (!shouldAttemptToRetryRead(retryReads, e)) {
                    if (retryReads) {
                        logUnableToRetry(command.getFirstKey(), e);
                    }
                    throw exception;
                }
            }
        } finally {
            connection.release();
        }

        return retryCommand(clientSideOperationTimeout, binding, database, commandCreator, decoder, transformer, exception);
    }

    private static <D, T> T retryCommand(final ClientSideOperationTimeout clientSideOperationTimeout, final ReadBinding binding,
                                         final String database, final CommandCreator commandCreator, final Decoder<D> decoder,
                                         final CommandReadTransformer<D, T> transformer, final MongoException originalException) {
        return withReleasableConnection(clientSideOperationTimeout, binding, originalException,
                (clientSideOperationTimeout1, source, connection) -> {
            try {
                if (!canRetryRead(source.getServerDescription(), connection.getDescription(), binding.getSessionContext())) {
                    throw originalException;
                }
                BsonDocument retryCommand = commandCreator.create(clientSideOperationTimeout1, source.getServerDescription(),
                        connection.getDescription());
                logRetryExecute(retryCommand.getFirstKey(), originalException);
                return executeCommand(clientSideOperationTimeout, database, retryCommand, decoder, source, connection,
                        binding.getReadPreference(), transformer, binding.getSessionContext(), source.getServerApi());
            } finally {
                connection.release();
            }
        });
    }


    /* Write Binding Helpers */
    static BsonDocument executeCommand(final ClientSideOperationTimeout clientSideOperationTimeout, final WriteBinding binding,
                                       final String database, final CommandCreator commandCreator) {
        return executeCommand(clientSideOperationTimeout, binding, database, commandCreator, new IdentityWriteTransformer<>());
    }

    static <T> T executeCommand(final ClientSideOperationTimeout clientSideOperationTimeout, final WriteBinding binding,
                                final String database, final CommandCreator commandCreator, final Decoder<T> decoder) {
        return executeCommand(clientSideOperationTimeout, binding, database, commandCreator, decoder, new IdentityWriteTransformer<>());
    }

    static <T> T executeCommand(final ClientSideOperationTimeout clientSideOperationTimeout, final WriteBinding binding,
                                final String database, final CommandCreator commandCreator,
                                final CommandWriteTransformer<BsonDocument, T> transformer) {
        return executeCommand(clientSideOperationTimeout, binding, database, commandCreator, new BsonDocumentCodec(), transformer);
    }

    static <D, T> T executeCommand(final ClientSideOperationTimeout clientSideOperationTimeout, final WriteBinding binding,
                                   final String database, final CommandCreator commandCreator, final Decoder<D> decoder,
                                   final CommandWriteTransformer<D, T> transformer) {
        return executeCommand(clientSideOperationTimeout, binding, database, commandCreator, new NoOpFieldNameValidator(), decoder,
                transformer);
    }

    static <T> T executeCommand(final ClientSideOperationTimeout clientSideOperationTimeout, final WriteBinding binding,
                                final String database, final BsonDocument command, final Connection connection,
                                final CommandWriteTransformer<BsonDocument, T> transformer) {
        return executeCommand(clientSideOperationTimeout, binding, database, command, new BsonDocumentCodec(), connection,
                transformer);
    }

    static <T> T executeCommand(final ClientSideOperationTimeout clientSideOperationTimeout, final WriteBinding binding,
                                final String database, final BsonDocument command, final Decoder<BsonDocument> decoder,
                                final Connection connection, final CommandWriteTransformer<BsonDocument, T> transformer) {
        notNull("binding", binding);
        return executeWriteCommand(clientSideOperationTimeout, database, command, decoder, connection, primary(), transformer,
                binding.getSessionContext(), binding.getServerApi());
    }

    static <T> T executeCommand(final ClientSideOperationTimeout clientSideOperationTimeout, final WriteBinding binding,
                                final String database, final BsonDocument command, final FieldNameValidator fieldNameValidator,
                                final Decoder<BsonDocument> decoder, final Connection connection,
                                final CommandWriteTransformer<BsonDocument, T> transformer) {
        notNull("binding", binding);
        return executeWriteCommand(clientSideOperationTimeout, database, command, fieldNameValidator, decoder, connection, primary(),
                transformer, binding.getSessionContext(), binding.getServerApi());
    }

    static <D, T> T executeCommand(final ClientSideOperationTimeout clientSideOperationTimeout, final WriteBinding binding,
                                   final String database, final CommandCreator commandCreator, final FieldNameValidator fieldNameValidator,
                                   final Decoder<D> decoder, final CommandWriteTransformer<D, T> transformer) {
        return withConnection(clientSideOperationTimeout, binding,
                (clientSideOperationTimeout1, source, connection) ->
                    transformer.apply(clientSideOperationTimeout1, connection, executeCommand(clientSideOperationTimeout1, database,
                            commandCreator.create(clientSideOperationTimeout1, source.getServerDescription(),
                                    connection.getDescription()), fieldNameValidator, decoder, source, connection, primary()))
                );
    }

    static BsonDocument executeCommand(final ClientSideOperationTimeout clientSideOperationTimeout, final WriteBinding binding,
                                       final String database, final BsonDocument command, final Connection connection) {
        notNull("binding", binding);
        return executeWriteCommand(clientSideOperationTimeout, database, command, new BsonDocumentCodec(), connection, primary(),
                binding.getSessionContext(), binding.getServerApi());
    }

    /* Private Connection Helpers */
    private static <T> T executeCommand(final ClientSideOperationTimeout clientSideOperationTimeout, final String database,
                                        final BsonDocument command, final FieldNameValidator fieldNameValidator, final Decoder<T> decoder,
                                        final ConnectionSource source, final Connection connection, final ReadPreference readPreference) {
        return executeCommand(clientSideOperationTimeout, database, command, fieldNameValidator, decoder, source, connection,
                readPreference, new IdentityReadTransformer<>(), source.getSessionContext(), source.getServerApi()
        );
    }

    private static <D, T> T executeCommand(final ClientSideOperationTimeout clientSideOperationTimeout, final String database,
                                           final BsonDocument command, final Decoder<D> decoder, final ConnectionSource source,
                                           final Connection connection, final ReadPreference readPreference,
                                           final CommandReadTransformer<D, T> transformer, final SessionContext sessionContext,
                                           final ServerApi serverApi) {
        return executeCommand(clientSideOperationTimeout, database, command, new NoOpFieldNameValidator(), decoder, source, connection,
                readPreference, transformer, sessionContext, serverApi);
    }

    private static <D, T> T executeCommand(final ClientSideOperationTimeout clientSideOperationTimeout, final String database,
                                           final BsonDocument command, final FieldNameValidator fieldNameValidator,
                                           final Decoder<D> decoder, final ConnectionSource source, final Connection connection,
                                           final ReadPreference readPreference, final CommandReadTransformer<D, T> transformer,
                                           final SessionContext sessionContext, final ServerApi serverApi) {
        return transformer.apply(clientSideOperationTimeout, source, connection, connection.command(database, command, fieldNameValidator,
                readPreference, decoder, sessionContext, serverApi));
    }

    private static <T> T executeWriteCommand(final ClientSideOperationTimeout clientSideOperationTimeout, final String database,
                                             final BsonDocument command, final Decoder<T> decoder, final Connection connection,
                                             final ReadPreference readPreference, final SessionContext sessionContext,
                                             final ServerApi serverApi) {
        return executeWriteCommand(clientSideOperationTimeout, database, command, new NoOpFieldNameValidator(), decoder, connection,
                readPreference, new IdentityWriteTransformer<>(), sessionContext, serverApi);
    }

    private static <D, T> T executeWriteCommand(final ClientSideOperationTimeout clientSideOperationTimeout, final String database,
                                                final BsonDocument command, final Decoder<D> decoder, final Connection connection,
                                                final ReadPreference readPreference, final CommandWriteTransformer<D, T> transformer,
                                                final SessionContext sessionContext, final ServerApi serverApi) {
        return executeWriteCommand(clientSideOperationTimeout, database, command, new NoOpFieldNameValidator(), decoder, connection,
                readPreference, transformer, sessionContext, serverApi);
    }

    private static <D, T> T executeWriteCommand(final ClientSideOperationTimeout clientSideOperationTimeout, final String database,
                                                final BsonDocument command, final FieldNameValidator fieldNameValidator,
                                                final Decoder<D> decoder, final Connection connection, final ReadPreference readPreference,
                                                final CommandWriteTransformer<D, T> transformer, final SessionContext sessionContext,
                                                final ServerApi serverApi) {
        return transformer.apply(clientSideOperationTimeout, connection, connection.command(database, command, fieldNameValidator,
                readPreference, decoder, sessionContext, serverApi));
    }

    /* Retryable write helpers */
    static <T, R> R executeRetryableCommand(final ClientSideOperationTimeout clientSideOperationTimeout, final WriteBinding binding,
                                            final String database, final ReadPreference readPreference,
                                            final FieldNameValidator fieldNameValidator, final Decoder<T> commandResultDecoder,
                                            final CommandCreator commandCreator, final CommandWriteTransformer<T, R> transformer) {
        return executeRetryableCommand(clientSideOperationTimeout, binding, database, readPreference, fieldNameValidator,
                commandResultDecoder, commandCreator, transformer, noOpRetryCommandModifier());
    }

    static <T, R> R executeRetryableCommand(final ClientSideOperationTimeout clientSideOperationTimeout, final WriteBinding binding,
                                            final String database, final ReadPreference readPreference,
                                            final FieldNameValidator fieldNameValidator, final Decoder<T> commandResultDecoder,
                                            final CommandCreator commandCreator, final CommandWriteTransformer<T, R> transformer,
                                            final Function<BsonDocument, BsonDocument> retryCommandModifier) {
        return withReleasableConnection(clientSideOperationTimeout, binding, (clientSideOperationTimeout1, source, connection) -> {
            BsonDocument command = null;
            MongoException exception;
            try {
                command = commandCreator.create(clientSideOperationTimeout1, source.getServerDescription(), connection.getDescription());
                return transformer.apply(clientSideOperationTimeout1, connection, connection.command(database, command, fieldNameValidator,
                        readPreference, commandResultDecoder, binding.getSessionContext(), binding.getServerApi()));
            } catch (MongoException e) {
                exception = e;
                if (!shouldAttemptToRetryWrite(command, e, connection.getDescription().getMaxWireVersion())) {
                    if (isRetryWritesEnabled(command)) {
                        logUnableToRetry(command.getFirstKey(), e);
                    }
                    throw transformWriteException(exception);
                }
            } finally {
                connection.release();
            }

            if (binding.getSessionContext().hasActiveTransaction()) {
                binding.getSessionContext().clearTransactionContext();
            }
            final BsonDocument originalCommand = command;
            final MongoException originalException = exception;
            return withReleasableConnection(clientSideOperationTimeout, binding, originalException,
                    (clientSideOperationTimeout2, source1, connection1) -> {
                try {
                    if (!canRetryWrite(source1.getServerDescription(), connection1.getDescription(), binding.getSessionContext())) {
                        throw originalException;
                    }
                    BsonDocument retryCommand = retryCommandModifier.apply(originalCommand);
                    logRetryExecute(retryCommand.getFirstKey(), originalException);
                    try {
                        return transformer.apply(clientSideOperationTimeout2, connection1, connection1.command(database, retryCommand,
                                fieldNameValidator, readPreference, commandResultDecoder, binding.getSessionContext(),
                                binding.getServerApi())
                        );
                    } catch (MongoException e) {
                        addRetryableWriteErrorLabel(e, connection1.getDescription().getMaxWireVersion());
                        throw e;
                    }
                } finally {
                    connection1.release();
                }
            });
        });
    }

    private SyncCommandOperationHelper() {
    }
}
