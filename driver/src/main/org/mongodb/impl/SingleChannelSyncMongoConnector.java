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
 *
 */

package org.mongodb.impl;

import org.mongodb.Document;
import org.mongodb.MongoClientOptions;
import org.mongodb.MongoCursorNotFoundException;
import org.mongodb.MongoNamespace;
import org.mongodb.MongoQueryFailureException;
import org.mongodb.ServerAddress;
import org.mongodb.WriteConcern;
import org.mongodb.async.SingleResultCallback;
import org.mongodb.command.GetLastError;
import org.mongodb.command.MongoCommandFailureException;
import org.mongodb.io.BufferPool;
import org.mongodb.io.MongoGateway;
import org.mongodb.io.PooledByteBufferOutputBuffer;
import org.mongodb.io.ResponseBuffers;
import org.mongodb.operation.MongoGetMore;
import org.mongodb.command.MongoCommand;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.MongoInsert;
import org.mongodb.operation.MongoKillCursor;
import org.mongodb.operation.MongoRemove;
import org.mongodb.operation.MongoReplace;
import org.mongodb.operation.MongoUpdate;
import org.mongodb.pool.SimplePool;
import org.mongodb.protocol.MongoCommandMessage;
import org.mongodb.protocol.MongoDeleteMessage;
import org.mongodb.protocol.MongoGetMoreMessage;
import org.mongodb.protocol.MongoInsertMessage;
import org.mongodb.protocol.MongoKillCursorsMessage;
import org.mongodb.protocol.MongoQueryMessage;
import org.mongodb.protocol.MongoReplaceMessage;
import org.mongodb.protocol.MongoReplyMessage;
import org.mongodb.protocol.MongoRequestMessage;
import org.mongodb.protocol.MongoUpdateMessage;
import org.mongodb.result.CommandResult;
import org.mongodb.result.QueryResult;
import org.mongodb.result.ServerCursor;
import org.mongodb.result.WriteResult;
import org.mongodb.Decoder;
import org.mongodb.Encoder;
import org.mongodb.Codec;
import org.mongodb.codecs.DocumentCodec;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;

final class SingleChannelSyncMongoConnector implements MongoPoolableConnector {
    private final BufferPool<ByteBuffer> bufferPool;
    private final MongoClientOptions options;
    private final SimplePool<MongoPoolableConnector> channelPool;
    private MongoGateway channel;

    SingleChannelSyncMongoConnector(final ServerAddress serverAddress, final SimplePool<MongoPoolableConnector> channelPool,
                                    final BufferPool<ByteBuffer> bufferPool, final MongoClientOptions options) {
        this.channelPool = channelPool;
        this.bufferPool = bufferPool;
        this.options = options;
        this.channel = MongoGateway.create(serverAddress, bufferPool, options);
    }

    @Override
    public CommandResult command(final String database, final MongoCommand commandOperation, final Codec<Document> codec) {
        commandOperation.readPreferenceIfAbsent(options.getReadPreference());
        final PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(bufferPool);
        try {
            final MongoCommandMessage message = new MongoCommandMessage(database + ".$cmd", commandOperation,
                    withDocumentEncoder(codec));
            message.encode(buffer);
            final ResponseBuffers responseBuffers = channel.sendAndReceiveMessage(buffer);
            try {
                MongoReplyMessage<Document> replyMessage = new MongoReplyMessage<Document>(responseBuffers, codec);
                return createCommandResult(commandOperation, replyMessage);
            } finally {
                responseBuffers.close();
            }
        } finally {
            buffer.close();
        }
    }

    private CommandResult createCommandResult(final MongoCommand commandOperation, final MongoReplyMessage<Document> replyMessage) {
        CommandResult commandResult = new CommandResult(commandOperation.toDocument(), channel.getAddress(),
                replyMessage.getDocuments().get(0), replyMessage.getElapsedNanoseconds());
        if (!commandResult.isOk()) {
            throw new MongoCommandFailureException(commandResult);
        }

        return commandResult;
    }

    @Override
    public <T> QueryResult<T> query(final MongoNamespace namespace, final MongoFind find,
                                    final Encoder<Document> queryEncoder, final Decoder<T> resultDecoder) {
        find.readPreferenceIfAbsent(options.getReadPreference());
        final PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(bufferPool);
        try {
            final MongoQueryMessage message = new MongoQueryMessage(namespace.getFullName(), find, withDocumentEncoder(queryEncoder));
            message.encode(buffer);

            final ResponseBuffers responseBuffers = channel.sendAndReceiveMessage(buffer);
            try {
                if (responseBuffers.getReplyHeader().isQueryFailure()) {
                    final Document errorDocument =
                            new MongoReplyMessage<Document>(responseBuffers, withDocumentSerializer(null)).getDocuments().get(0);
                    throw new MongoQueryFailureException(channel.getAddress(), errorDocument);
                }
                final MongoReplyMessage<T> replyMessage = new MongoReplyMessage<T>(responseBuffers, resultDecoder);
                return new QueryResult<T>(replyMessage, channel.getAddress());
            } finally {
                responseBuffers.close();
            }
        } finally {
            buffer.close();
        }
    }

    @Override
    public <T> QueryResult<T> getMore(final MongoNamespace namespace, final MongoGetMore getMore,
                                      final Decoder<T> resultDecoder) {
        final PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(bufferPool);
        try {
            final MongoGetMoreMessage message = new MongoGetMoreMessage(namespace.getFullName(), getMore);
            message.encode(buffer);
            final ResponseBuffers responseBuffers = channel.sendAndReceiveMessage(buffer);
            try {
                if (responseBuffers.getReplyHeader().isCursorNotFound()) {
                    throw new MongoCursorNotFoundException(new ServerCursor(message.getCursorId(), channel.getAddress()));
                }

                final MongoReplyMessage<T> replyMessage = new MongoReplyMessage<T>(responseBuffers, resultDecoder);
                return new QueryResult<T>(replyMessage, channel.getAddress());
            } finally {
                responseBuffers.close();
            }
        } finally {
            buffer.close();
        }
    }

    @Override
    public <T> WriteResult insert(final MongoNamespace namespace, final MongoInsert<T> insert,
                                  final Encoder<T> encoder) {
        insert.writeConcernIfAbsent(options.getWriteConcern());
        final MongoInsertMessage<T> insertMessage = new MongoInsertMessage<T>(namespace.getFullName(), insert, encoder);
        return new WriteResult(insert, sendWriteMessage(namespace, insertMessage, insert.getWriteConcern()));
    }

    @Override
    public WriteResult update(final MongoNamespace namespace, final MongoUpdate update,
                              final Encoder<Document> queryEncoder) {
        update.writeConcernIfAbsent(options.getWriteConcern());
        final MongoUpdateMessage message = new MongoUpdateMessage(namespace.getFullName(), update, withDocumentEncoder(queryEncoder));
        return new WriteResult(update, sendWriteMessage(namespace, message, update.getWriteConcern()));
    }

    @Override
    public <T> WriteResult replace(final MongoNamespace namespace, final MongoReplace<T> replace,
                                   final Encoder<Document> queryEncoder, final Encoder<T> encoder) {
        replace.writeConcernIfAbsent(options.getWriteConcern());
        final MongoReplaceMessage<T> message = new MongoReplaceMessage<T>(namespace.getFullName(), replace,
                withDocumentEncoder(queryEncoder), encoder);
        return new WriteResult(replace, sendWriteMessage(namespace, message, replace.getWriteConcern()));
    }

    @Override
    public WriteResult remove(final MongoNamespace namespace, final MongoRemove remove,
                              final Encoder<Document> queryEncoder) {
        remove.writeConcernIfAbsent(options.getWriteConcern());
        final MongoDeleteMessage message = new MongoDeleteMessage(namespace.getFullName(), remove,
                withDocumentEncoder(queryEncoder));
        return new WriteResult(remove, sendWriteMessage(namespace, message, remove.getWriteConcern()));
    }

    @Override
    public void killCursors(final MongoKillCursor killCursor) {
        final PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(bufferPool);
        try {
            final MongoKillCursorsMessage message = new MongoKillCursorsMessage(killCursor);
            message.encode(buffer);
            channel.sendMessage(buffer);
        } finally {
            buffer.close();
        }
    }

    @Override
    public void close() {
        if (channel != null) {
            channel.close();
            channel = null;
        }
    }

    @Override
    public void release() {
        if (channel == null) {
            throw new IllegalStateException("Can not release a channel that's already closed");
        }
        if (channelPool == null) {
            throw new IllegalStateException("Can not release a channel not associated with a pool");
        }

        channelPool.done(this);
    }

    @Override
    public List<ServerAddress> getServerAddressList() {
        return Arrays.asList(channel.getAddress());
    }

    private Codec<Document> withDocumentSerializer(final Codec<Document> codec) {
        if (codec != null) {
            return codec;
        }
        return new DocumentCodec(options.getPrimitiveCodecs());
    }

    private Encoder<Document> withDocumentEncoder(final Encoder<Document> encoder) {
        if (encoder != null) {
            return encoder;
        }
        return new DocumentCodec(options.getPrimitiveCodecs());
    }

    private CommandResult sendWriteMessage(final MongoNamespace namespace, final MongoRequestMessage writeMessage,
                                           final WriteConcern writeConcern) {
        PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(bufferPool);
        try {
            writeMessage.encode(buffer);
            if (writeConcern.callGetLastError()) {
                final GetLastError getLastError = new GetLastError(writeConcern);
                final DocumentCodec codec = new DocumentCodec(options.getPrimitiveCodecs());
                MongoCommandMessage getLastErrorMessage = new MongoCommandMessage(namespace.getDatabaseName() + ".$cmd", getLastError,
                        codec);
                getLastErrorMessage.encode(buffer);
                ResponseBuffers responseBuffers = channel.sendAndReceiveMessage(buffer);
                try {
                    return getLastError.parseGetLastErrorResponse(createCommandResult(getLastError,
                            new MongoReplyMessage<Document>(responseBuffers, codec)));
                } finally {
                    responseBuffers.close();
                }
            }
            else {
                channel.sendMessage(buffer);
                return null;
            }
        } finally {
            buffer.close();
        }
    }

    @Override
    public Future<CommandResult> asyncCommand(final String database, final MongoCommand commandOperation,
                                              final Codec<Document> codec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void asyncCommand(final String database, final MongoCommand commandOperation,
                             final Codec<Document> codec, final SingleResultCallback<CommandResult> callback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Future<QueryResult<T>> asyncQuery(final MongoNamespace namespace, final MongoFind find,
                                                 final Encoder<Document> queryEncoder, final Decoder<T> resultDecoder) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> void asyncQuery(final MongoNamespace namespace, final MongoFind find, final Encoder<Document> queryEncoder,
                               final Decoder<T> resultDecoder, final SingleResultCallback<QueryResult<T>> callback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Future<QueryResult<T>> asyncGetMore(final MongoNamespace namespace, final MongoGetMore getMore,
                                                   final Decoder<T> resultDecoder) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> void asyncGetMore(final MongoNamespace namespace, final MongoGetMore getMore, final Decoder<T> resultDecoder,
                                 final SingleResultCallback<QueryResult<T>> callback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Future<WriteResult> asyncInsert(final MongoNamespace namespace, final MongoInsert<T> insert,
                                               final Encoder<T> encoder) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> void asyncInsert(final MongoNamespace namespace, final MongoInsert<T> insert, final Encoder<T> encoder,
                                final SingleResultCallback<WriteResult> callback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Future<WriteResult> asyncUpdate(final MongoNamespace namespace, final MongoUpdate update,
                                           final Encoder<Document> queryEncoder) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void asyncUpdate(final MongoNamespace namespace, final MongoUpdate update, final Encoder<Document> queryEncoder,
                            final SingleResultCallback<WriteResult> callback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Future<WriteResult> asyncReplace(final MongoNamespace namespace, final MongoReplace<T> replace,
                                                final Encoder<Document> queryEncoder, final Encoder<T> encoder) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> void asyncReplace(final MongoNamespace namespace, final MongoReplace<T> replace,
                                 final Encoder<Document> queryEncoder, final Encoder<T> encoder,
                                 final SingleResultCallback<WriteResult> callback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Future<WriteResult> asyncRemove(final MongoNamespace namespace, final MongoRemove remove,
                                           final Encoder<Document> queryEncoder) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void asyncRemove(final MongoNamespace namespace, final MongoRemove remove, final Encoder<Document> queryEncoder,
                            final SingleResultCallback<WriteResult> callback) {
        throw new UnsupportedOperationException();
    }
}
