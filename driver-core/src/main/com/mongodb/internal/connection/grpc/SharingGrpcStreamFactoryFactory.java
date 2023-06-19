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
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.grpc.GrpcStreamFactoryFactory;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.connection.StreamFactory;
import com.mongodb.connection.StreamFactoryFactory;
import com.mongodb.lang.Nullable;
import io.grpc.Channel;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import org.bson.BsonDocument;

import javax.net.ssl.SSLException;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.mongodb.internal.connection.netty.NettyChannelOptionsSetter.configureNettyChannelOptions;

/**
 * A {@link StreamFactory} that is similar to {@link GrpcStreamFactoryFactory},
 * but shares gRPC {@link Channel}s between all {@link StreamFactory}s
 * {@linkplain #create(SocketSettings, SslSettings) created} by it.
 * Consequently, it must not be shared between {@code MongoClient}s.
 * <p>
 * This class is not part of the public API and may be removed or changed at any time.</p>
 */
@ThreadSafe
public final class SharingGrpcStreamFactoryFactory implements StreamFactoryFactory {
    private final GrpcStreamFactoryFactory wrapped;
    private final ConnectionPoolSettings connectionPoolSettings;
    @Nullable
    private final BsonDocument clientMetadataDocument;
    private final Channels sharedChannels;

    public SharingGrpcStreamFactoryFactory(final GrpcStreamFactoryFactory wrapped, final ConnectionPoolSettings connectionPoolSettings) {
        this(wrapped, connectionPoolSettings, null);
    }

    private SharingGrpcStreamFactoryFactory(
            final GrpcStreamFactoryFactory wrapped,
            final ConnectionPoolSettings connectionPoolSettings,
            @Nullable final BsonDocument clientMetadataDocument) {
        this.wrapped = wrapped;
        this.connectionPoolSettings = connectionPoolSettings;
        this.clientMetadataDocument = clientMetadataDocument;
        sharedChannels = new Channels();
    }

    /**
     * @return The configured {@link SharingGrpcStreamFactoryFactory}, which may or may not be the same as {@code this}.
     */
    public SharingGrpcStreamFactoryFactory withClientMetadataDocument(@Nullable final BsonDocument clientMetadataDocument) {
        return clientMetadataDocument == null ? this : new SharingGrpcStreamFactoryFactory(wrapped, connectionPoolSettings, clientMetadataDocument);
    }

    @Override
    public StreamFactory create(final SocketSettings socketSettings, final SslSettings sslSettings) {
        GrpcStreamFactory streamFactoryFromWrapped = (GrpcStreamFactory) wrapped.create(socketSettings, sslSettings);
        return new GrpcStreamFactory(socketSettings, sslSettings, connectionPoolSettings, clientMetadataDocument,
                streamFactoryFromWrapped.clientId(), streamFactoryFromWrapped.allocator(), sharedChannels);
    }

    /**
     * Returns the wrapped {@link GrpcStreamFactoryFactory}.
     */
    public GrpcStreamFactoryFactory unwrap() {
        return wrapped;
    }

    @Override
    public String toString() {
        return "SharingGrpcStreamFactoryFactory{"
                + "wrapped=" + wrapped
                + '}';
    }

    // VAKOTODO how to release resources?
    @ThreadSafe
    public static final class Channels {
        private final ConcurrentHashMap<ServerAddress, ManagedChannel> channels;

        public Channels() {
            channels = new ConcurrentHashMap<>();
        }

        ManagedChannel channel(
                final ServerAddress serverAddress,
                final SocketSettings socketSettings,
                final SslSettings sslSettings,
                @Nullable
                final ConnectionPoolSettings connectionPoolSettings,
                final ByteBufAllocator allocator) {
            return channels.compute(serverAddress, (address, channel) -> {
                if (channel == null || channel.getState(false) == ConnectivityState.SHUTDOWN) {
                    return createChannel(serverAddress, socketSettings, sslSettings, connectionPoolSettings, allocator);
                } else {
                    return channel;
                }
            });
        }

        private static ManagedChannel createChannel(
                final ServerAddress serverAddress,
                final SocketSettings socketSettings,
                final SslSettings sslSettings,
                @Nullable
                final ConnectionPoolSettings connectionPoolSettings,
                final ByteBufAllocator allocator) {
            NettyChannelBuilder channelBuilder = NettyChannelBuilder.forAddress(serverAddress.getHost(), serverAddress.getPort())
                    .disableRetry();
            if (connectionPoolSettings != null) {
                long maxIdleMillis = connectionPoolSettings.getMaxConnectionIdleTime(TimeUnit.MICROSECONDS);
                if (maxIdleMillis > 0) {
                    channelBuilder.idleTimeout(maxIdleMillis, TimeUnit.MILLISECONDS);
                }
            }
            configureJunk(channelBuilder);
            configureTls(channelBuilder, sslSettings);
            configureNettyChannelOptions(socketSettings, allocator, channelBuilder::withOption);
            return channelBuilder.build();
        }

        private static void configureTls(final NettyChannelBuilder channelBuilder, final SslSettings sslSettings) {
            if (sslSettings.isEnabled()) {
                channelBuilder.negotiationType(NegotiationType.TLS);
                SslContext nettySslContext;
                try {
                    nettySslContext = SslContextBuilder.forClient()
                            .sslProvider(SslProvider.JDK)
                            .applicationProtocolConfig(new ApplicationProtocolConfig(
                                    ApplicationProtocolConfig.Protocol.ALPN,
                                    ApplicationProtocolConfig.SelectorFailureBehavior.FATAL_ALERT,
                                    ApplicationProtocolConfig.SelectedListenerFailureBehavior.FATAL_ALERT,
                                    // "h2" stands for HTTP/2, see `javax.net.ssl.SSLParameters`
                                    Collections.singletonList("h2"))
                            ).build();
                } catch (SSLException e) {
                    throw new RuntimeException(e);
                }
                channelBuilder.sslContext(nettySslContext);
            } else {
                channelBuilder.negotiationType(NegotiationType.PLAINTEXT);
            }
        }

        // VAKOTODO this is junk required by https://github.com/10gen/atlasproxy, delete in the future.
        private static void configureJunk(final NettyChannelBuilder channelBuilder) {
            channelBuilder.overrideAuthority("host.local.10gen.cc");
        }
    }
}
