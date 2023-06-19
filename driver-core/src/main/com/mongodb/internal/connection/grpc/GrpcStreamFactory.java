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

package com.mongodb.internal.connection.grpc;

import com.mongodb.ServerAddress;
import com.mongodb.annotations.Immutable;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.connection.BufferProvider;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.connection.StreamFactory;
import com.mongodb.internal.connection.ByteBufferBsonOutput;
import com.mongodb.internal.connection.MessageSettings;
import com.mongodb.internal.connection.grpc.GrpcStream.Marshallers;
import com.mongodb.internal.connection.grpc.GrpcStream.WriteState.PendingWrite;
import com.mongodb.internal.connection.netty.NettyByteBuf;
import com.mongodb.lang.Nullable;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.netty.buffer.ByteBufAllocator;
import org.bson.BsonBinaryWriter;
import org.bson.BsonBinaryWriterSettings;
import org.bson.BsonDocument;
import org.bson.BsonWriterSettings;
import org.bson.ByteBuf;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;

import java.io.InputStream;
import java.util.Base64;
import java.util.UUID;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

/**
 * A {@link StreamFactory} for {@link GrpcStream}s.
 *  <p>
 *  This class is not part of the public API and may be removed or changed at any time.</p>
 */
@ThreadSafe
public final class GrpcStreamFactory implements StreamFactory {
    /**
     * @see MessageSettings#DEFAULT_MAX_DOCUMENT_SIZE
     */
    // VAKOTODO use
    private static final int MAX_BSON_OBJECT_SIZE = MessageSettings.DEFAULT_MAX_DOCUMENT_SIZE;
    /**
     * @see MessageSettings#DEFAULT_MAX_MESSAGE_SIZE
     */
    static final int MAX_MESSAGE_SIZE_BYTES = 48_000_000;
    /**
     * @see MessageSettings#DEFAULT_MAX_BATCH_COUNT
     */
    // VAKOTODO use
    private static final int MAX_WRITE_BATCH_SIZE = 100_000;

    private final SocketSettings socketSettings;
    private final SslSettings sslSettings;
    @Nullable
    private final ConnectionPoolSettings connectionPoolSettings;
    private final UUID clientId;
    private final NettyByteBufProvider bufferProvider;
    @Nullable
    private final ClientMetadataDocument clientMetadataDocument;
    private final SharingGrpcStreamFactoryFactory.Channels channels;

    /**
     * @param connectionPoolSettings Not {@code null} iff this constructor is called from {@link SharingGrpcStreamFactoryFactory}.
     * @param clientMetadataDocument May be non-{@code null} only if this constructor is called from {@link SharingGrpcStreamFactoryFactory}.
     * @param clientId The value for the {@code mongodb-clientId} {@linkplain Metadata.Key metadata key}.
     */
    public GrpcStreamFactory(
            final SocketSettings socketSettings,
            final SslSettings sslSettings,
            @Nullable
            final ConnectionPoolSettings connectionPoolSettings,
            @Nullable
            final BsonDocument clientMetadataDocument,
            final UUID clientId,
            final ByteBufAllocator allocator,
            final SharingGrpcStreamFactoryFactory.Channels channels) {
        this.socketSettings = socketSettings;
        this.sslSettings = sslSettings;
        this.connectionPoolSettings = connectionPoolSettings;
        this.clientId = clientId;
        bufferProvider = new NettyByteBufProvider(allocator);
        this.clientMetadataDocument = clientMetadataDocument == null ? null : new ClientMetadataDocument(clientMetadataDocument, bufferProvider);
        this.channels = channels;
    }

    @Override
    public GrpcStream create(final ServerAddress serverAddress) {
        return new GrpcStream(serverAddress, clientId, clientMetadataDocument, socketSettings, bufferProvider,
                (fullMethodName, marshallers) -> createCall(serverAddress, fullMethodName, marshallers));
    }

    public UUID clientId() {
        return clientId;
    }

    public ByteBufAllocator allocator() {
        return bufferProvider.allocator();
    }

    private ClientCall<PendingWrite, InputStream> createCall(
            final ServerAddress serverAddress,
            final String fullMethodName,
            final Marshallers<PendingWrite, InputStream> marshallers) {
        ManagedChannel channel = channels.channel(serverAddress, socketSettings, sslSettings, connectionPoolSettings, allocator());
        return channel.newCall(
                MethodDescriptor.newBuilder(marshallers.marshaller(), marshallers.unmarshaller())
                        .setFullMethodName(fullMethodName)
                        .setType(MethodDescriptor.MethodType.BIDI_STREAMING)
                        .setSafe(false)
                        .setIdempotent(false)
                        .build(),
                CallOptions.DEFAULT
                        .withMaxOutboundMessageSize(MAX_MESSAGE_SIZE_BYTES)
                        .withMaxInboundMessageSize(MAX_MESSAGE_SIZE_BYTES)
        );
    }

    static final class NettyByteBufProvider implements BufferProvider {
        private final ByteBufAllocator allocator;

        NettyByteBufProvider(final ByteBufAllocator allocator) {
            this.allocator = allocator;
        }

        @Override
        public ByteBuf getBuffer(final int size) {
            io.netty.buffer.ByteBuf allocatedBuffer = allocator.buffer(size, size);
            allocatedBuffer.touch();
            return new NettyByteBuf(allocatedBuffer);
        }

        ByteBufAllocator allocator() {
            return allocator;
        }
    }

    @Immutable
    static final class ClientMetadataDocument {
        private static final CodecRegistry CODEC_REGISTRY = fromProviders(new BsonValueCodecProvider());

        private final String base64;

        private ClientMetadataDocument(final BsonDocument clientMetadataDocument, final BufferProvider bufferProvider) {
            try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(bufferProvider)) {
                CODEC_REGISTRY.get(BsonDocument.class).encode(
                        new BsonBinaryWriter(new BsonWriterSettings(), new BsonBinaryWriterSettings(MAX_BSON_OBJECT_SIZE), out),
                        clientMetadataDocument,
                        EncoderContext.builder().build());
                base64 = Base64.getEncoder().encodeToString(out.toByteArray());
            }
        }

        String base64() {
            return base64;
        }
    }
}
