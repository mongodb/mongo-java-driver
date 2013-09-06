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

package org.mongodb.protocol;

import org.mongodb.Decoder;
import org.mongodb.Document;
import org.mongodb.MongoCursorNotFoundException;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.MongoQueryFailureException;
import org.mongodb.ServerCursor;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.Channel;
import org.mongodb.connection.ChannelReceiveArgs;
import org.mongodb.connection.PooledByteBufferOutputBuffer;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.ServerDescription;
import org.mongodb.diagnostics.Loggers;
import org.mongodb.operation.GetMore;
import org.mongodb.operation.SingleResultFuture;
import org.mongodb.operation.SingleResultFutureCallback;
import org.mongodb.protocol.message.GetMoreMessage;
import org.mongodb.protocol.message.ReplyMessage;

import java.util.logging.Logger;

import static java.lang.String.format;
import static org.mongodb.protocol.ProtocolHelper.encodeMessageToBuffer;
import static org.mongodb.protocol.ProtocolHelper.getMessageSettings;

public class GetMoreProtocol<T> implements Protocol<QueryResult<T>> {

    public static final Logger LOGGER = Loggers.getLogger("protocol.getmore");

    private final GetMore getMore;
    private final Decoder<T> resultDecoder;
    private ServerDescription serverDescription;
    private final Channel channel;
    private final boolean closeChannel;
    private final MongoNamespace namespace;
    private final BufferProvider bufferProvider;

    public GetMoreProtocol(final MongoNamespace namespace, final GetMore getMore, final Decoder<T> resultDecoder,
                           final BufferProvider bufferProvider, final ServerDescription serverDescription, final Channel channel,
                           final boolean closeChannel) {
        this.namespace = namespace;
        this.bufferProvider = bufferProvider;
        this.getMore = getMore;
        this.resultDecoder = resultDecoder;
        this.serverDescription = serverDescription;
        this.channel = channel;
        this.closeChannel = closeChannel;
    }

    @Override
    public QueryResult<T> execute() {
        try {
            LOGGER.fine(format("Getting more documents from cursor with id %d on connection [%s] to server %s",
                    getMore.getServerCursor().getId(), channel.getId(), channel.getServerAddress()));
            QueryResult<T> queryResult = receiveMessage(sendMessage());
            LOGGER.fine("Get-more completed");
            return queryResult;
        } finally {
            if (closeChannel) {
                channel.close();
            }
        }
    }

    public MongoFuture<QueryResult<T>> executeAsync() {
        final SingleResultFuture<QueryResult<T>> retVal = new SingleResultFuture<QueryResult<T>>();

        final PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(bufferProvider);
        final GetMoreMessage message = new GetMoreMessage(namespace.getFullName(), getMore,
                getMessageSettings(serverDescription));
        encodeMessageToBuffer(message, buffer);
        channel.sendMessageAsync(buffer.getByteBuffers(),
                new SendMessageCallback<QueryResult<T>>(channel, buffer, message.getId(), retVal,
                        new GetMoreResultCallback<T>(
                                new SingleResultFutureCallback<QueryResult<T>>(retVal), resultDecoder,
                                getMore.getServerCursor().getId(), message.getId(), channel, closeChannel)));
        return retVal;
    }


    private GetMoreMessage sendMessage() {
        final PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(bufferProvider);
        try {
            final GetMoreMessage message = new GetMoreMessage(namespace.getFullName(), getMore,
                    getMessageSettings(serverDescription));
            message.encode(buffer);
            channel.sendMessage(buffer.getByteBuffers());
            return message;
        } finally {
            buffer.close();
        }
    }

    private QueryResult<T> receiveMessage(final GetMoreMessage message) {
        final ResponseBuffers responseBuffers = channel.receiveMessage(
                new ChannelReceiveArgs(message.getId()));
        try {
            if (responseBuffers.getReplyHeader().isCursorNotFound()) {
                throw new MongoCursorNotFoundException(new ServerCursor(message.getCursorId(), channel.getServerAddress()));
            }

            if (responseBuffers.getReplyHeader().isQueryFailure()) {
                final Document errorDocument =
                        new ReplyMessage<Document>(responseBuffers, new DocumentCodec(), message.getId()).getDocuments().get(0);
                throw new MongoQueryFailureException(channel.getServerAddress(), errorDocument);
            }

            return new QueryResult<T>(new ReplyMessage<T>(responseBuffers, resultDecoder, message.getId()),
                    channel.getServerAddress());
        } finally {
            responseBuffers.close();
        }
    }
}
