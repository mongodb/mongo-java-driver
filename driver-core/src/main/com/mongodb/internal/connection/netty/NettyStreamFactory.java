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

package com.mongodb.internal.connection.netty;

import com.mongodb.ServerAddress;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.internal.connection.DefaultInetAddressResolver;
import com.mongodb.internal.connection.Stream;
import com.mongodb.internal.connection.StreamFactory;
import com.mongodb.lang.Nullable;
import com.mongodb.spi.dns.InetAddressResolver;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A StreamFactory for Streams based on <a href="http://netty.io/">Netty</a> 4.x.
 */
public class NettyStreamFactory implements StreamFactory {
    private final InetAddressResolver inetAddressResolver;
    private final SocketSettings settings;
    private final SslSettings sslSettings;
    private final EventLoopGroup eventLoopGroup;
    private final Class<? extends SocketChannel> socketChannelClass;
    private final ByteBufAllocator allocator;
    @Nullable
    private final SslContext sslContext;

    /**
     * Construct a new instance of the factory.
     *
     * @param settings the socket settings
     * @param sslSettings the SSL settings
     * @param eventLoopGroup the event loop group that all channels created by this factory will be a part of
     * @param socketChannelClass the socket channel class
     * @param allocator the allocator to use for ByteBuf instances
     * @param sslContext the Netty {@link SslContext}
     *                   as specified by {@link NettyStreamFactoryFactory.Builder#sslContext(SslContext)}.
     */
    public NettyStreamFactory(final InetAddressResolver inetAddressResolver,  final SocketSettings settings,
            final SslSettings sslSettings, final EventLoopGroup eventLoopGroup, final Class<? extends SocketChannel> socketChannelClass,
            final ByteBufAllocator allocator, @Nullable final SslContext sslContext) {
        this.inetAddressResolver = inetAddressResolver;
        this.settings = notNull("settings", settings);
        this.sslSettings = notNull("sslSettings", sslSettings);
        this.eventLoopGroup = notNull("eventLoopGroup", eventLoopGroup);
        this.socketChannelClass = notNull("socketChannelClass", socketChannelClass);
        this.allocator = notNull("allocator", allocator);
        this.sslContext = sslContext;
    }

    /**
     * Construct a new instance of the factory.
     *
     * @param settings the socket settings
     * @param sslSettings the SSL settings
     * @param eventLoopGroup the event loop group that all channels created by this factory will be a part of
     * @param socketChannelClass the socket channel class
     * @param allocator the allocator to use for ByteBuf instances
     */
    public NettyStreamFactory(final SocketSettings settings, final SslSettings sslSettings,
                              final EventLoopGroup eventLoopGroup, final Class<? extends SocketChannel> socketChannelClass,
                              final ByteBufAllocator allocator) {
        this(new DefaultInetAddressResolver(), settings, sslSettings, eventLoopGroup, socketChannelClass, allocator, null);
    }

    /**
     * Construct a new instance of the factory.
     *
     * @param settings the socket settings
     * @param sslSettings the SSL settings
     * @param eventLoopGroup the event loop group that all channels created by this factory will be a part of
     * @param allocator the allocator to use for ByteBuf instances
     */
    public NettyStreamFactory(final SocketSettings settings, final SslSettings sslSettings, final EventLoopGroup eventLoopGroup,
                              final ByteBufAllocator allocator) {
        this(settings, sslSettings, eventLoopGroup, NioSocketChannel.class, allocator);
    }

    /**
     * Construct a new instance of the factory.
     *
     * @param settings the socket settings
     * @param sslSettings the SSL settings
     * @param eventLoopGroup the event loop group that all channels created by this factory will be a part of
     */
    public NettyStreamFactory(final SocketSettings settings, final SslSettings sslSettings, final EventLoopGroup eventLoopGroup) {
        this(settings, sslSettings, eventLoopGroup, PooledByteBufAllocator.DEFAULT);
    }

    /**
     * Construct a new instance of the factory with a default allocator, nio event loop group and nio socket channel.
     *
     * @param settings the socket settings
     * @param sslSettings the SSL settings
     */
    public NettyStreamFactory(final SocketSettings settings, final SslSettings sslSettings) {
        this(settings, sslSettings, new NioEventLoopGroup());
    }

    @Override
    public Stream create(final ServerAddress serverAddress) {
        return new NettyStream(serverAddress, inetAddressResolver, settings, sslSettings, eventLoopGroup, socketChannelClass, allocator,
                sslContext);
    }

}
