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

import com.mongodb.connection.NettyTransportSettings;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.internal.VisibleForTesting;
import com.mongodb.internal.connection.StreamFactory;
import com.mongodb.internal.connection.StreamFactoryFactory;
import com.mongodb.lang.Nullable;
import com.mongodb.spi.dns.InetAddressResolver;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.ReferenceCountedOpenSslClientContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;

import java.security.Security;
import java.util.Objects;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;

/**
 * A {@code StreamFactoryFactory} implementation for <a href="http://netty.io/">Netty</a>-based streams.
 */
public final class NettyStreamFactoryFactory implements StreamFactoryFactory {

    private final EventLoopGroup eventLoopGroup;
    private final boolean ownsEventLoopGroup;
    private final Class<? extends SocketChannel> socketChannelClass;
    private final ByteBufAllocator allocator;
    @Nullable
    private final SslContext sslContext;
    private final InetAddressResolver inetAddressResolver;

    /**
     * Gets a builder for an instance of {@code NettyStreamFactoryFactory}.
     * @return the builder
     */
    public static Builder builder() {
        return new Builder();
    }

    @VisibleForTesting(otherwise = PRIVATE)
    EventLoopGroup getEventLoopGroup() {
        return eventLoopGroup;
    }

    @VisibleForTesting(otherwise = PRIVATE)
    Class<? extends SocketChannel> getSocketChannelClass() {
        return socketChannelClass;
    }

    @VisibleForTesting(otherwise = PRIVATE)
    ByteBufAllocator getAllocator() {
        return allocator;
    }

    @VisibleForTesting(otherwise = PRIVATE)
    @Nullable
    SslContext getSslContext() {
        return sslContext;
    }

    /**
     * A builder for an instance of {@code NettyStreamFactoryFactory}.
     */
    public static final class Builder {
        private ByteBufAllocator allocator;
        private Class<? extends SocketChannel> socketChannelClass;
        private EventLoopGroup eventLoopGroup;
        @Nullable
        private SslContext sslContext;
        private InetAddressResolver inetAddressResolver;

        private Builder() {
        }

        /**
         * Apply NettyTransportSettings
         *
         * @param settings the settings
         * @return this
         */
        public Builder applySettings(final NettyTransportSettings settings) {
            this.allocator = settings.getAllocator();
            this.eventLoopGroup = settings.getEventLoopGroup();
            this.sslContext = settings.getSslContext();
            this.socketChannelClass = settings.getSocketChannelClass();
            return this;
        }


        /**
         * Sets the allocator.
         *
         * @param allocator the allocator to use for ByteBuf instances
         * @return this
         */
        public Builder allocator(final ByteBufAllocator allocator) {
            this.allocator = notNull("allocator", allocator);
            return this;
        }

        /**
         * Sets the socket channel class
         *
         * @param socketChannelClass the socket channel class
         * @return this
         */
        public Builder socketChannelClass(final Class<? extends SocketChannel> socketChannelClass) {
            this.socketChannelClass = notNull("socketChannelClass", socketChannelClass);
            return this;
        }

        /**
         * Sets the event loop group.
         *
         * <p>It is highly recommended to supply your own event loop group and manage its shutdown.  Otherwise, the event
         * loop group created by default will not be shutdown properly.</p>
         *
         * @param eventLoopGroup the event loop group that all channels created by this factory will be a part of
         * @return this
         */
        public Builder eventLoopGroup(final EventLoopGroup eventLoopGroup) {
            this.eventLoopGroup = notNull("eventLoopGroup", eventLoopGroup);
            return this;
        }

        /**
         * Sets a {@linkplain SslContextBuilder#forClient() client-side} {@link SslContext io.netty.handler.ssl.SslContext},
         * which overrides the standard {@link SslSettings#getContext()}.
         * By default it is {@code null} and {@link SslSettings#getContext()} is at play.
         * <p>
         * This option may be used as a convenient way to utilize
         * <a href="https://www.openssl.org/">OpenSSL</a> as an alternative to the TLS/SSL protocol implementation in a JDK.
         * To achieve this, specify {@link SslProvider#OPENSSL} TLS/SSL protocol provider via
         * {@link SslContextBuilder#sslProvider(SslProvider)}. Note that doing so adds a runtime dependency on
         * <a href="https://netty.io/wiki/forked-tomcat-native.html">netty-tcnative</a>, which you must satisfy.
         * <p>
         * Notes:
         * <ul>
         *    <li>Netty {@link SslContext} may not examine some
         *    {@linkplain Security security}/{@linkplain System#getProperties() system} properties that are used to
         *    <a href="https://docs.oracle.com/javase/8/docs/technotes/guides/security/jsse/JSSERefGuide.html#Customization">
         *    customize JSSE</a>. Therefore, instead of using them you may have to apply the equivalent configuration programmatically,
         *    if both the {@link SslContextBuilder} and the TLS/SSL protocol provider of choice support it.
         *    </li>
         *    <li>Only {@link SslProvider#JDK} and {@link SslProvider#OPENSSL} TLS/SSL protocol providers are supported.
         *    </li>
         * </ul>
         *
         * @param sslContext The Netty {@link SslContext}, which must be created via {@linkplain SslContextBuilder#forClient()}.
         * @return {@code this}.
         */
        public Builder sslContext(final SslContext sslContext) {
            this.sslContext = notNull("sslContext", sslContext);
            isTrueArgument("sslContext must be client-side", sslContext.isClient());
            isTrueArgument("sslContext must use either SslProvider.JDK or SslProvider.OPENSSL TLS/SSL protocol provider",
                    !(sslContext instanceof ReferenceCountedOpenSslClientContext));

            return this;
        }

        public Builder inetAddressResolver(final InetAddressResolver inetAddressResolver) {
            this.inetAddressResolver = inetAddressResolver;
            return this;
        }

        /**
         * Build an instance of {@code NettyStreamFactoryFactory}.
         * @return factory of the netty stream factory
         */
        public NettyStreamFactoryFactory build() {
            return new NettyStreamFactoryFactory(this);
        }
    }

    @Override
    public StreamFactory create(final SocketSettings socketSettings, final SslSettings sslSettings) {
        return new NettyStreamFactory(inetAddressResolver, socketSettings, sslSettings, eventLoopGroup, socketChannelClass, allocator,
                sslContext);
    }

    @Override
    public void close() {
         if (ownsEventLoopGroup) {
             // ignore the returned Future.  This is in line with MongoClient behavior to not block waiting for connections to be returned
             // to the pool
             eventLoopGroup.shutdownGracefully();
         }
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NettyStreamFactoryFactory that = (NettyStreamFactoryFactory) o;
        return Objects.equals(eventLoopGroup, that.eventLoopGroup) && Objects.equals(socketChannelClass, that.socketChannelClass)
                && Objects.equals(allocator, that.allocator) && Objects.equals(sslContext, that.sslContext)
                && Objects.equals(inetAddressResolver, that.inetAddressResolver);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventLoopGroup, socketChannelClass, allocator, sslContext, inetAddressResolver);
    }

    private NettyStreamFactoryFactory(final Builder builder) {
        allocator = builder.allocator == null ? ByteBufAllocator.DEFAULT : builder.allocator;
        socketChannelClass = builder.socketChannelClass == null ? NioSocketChannel.class : builder.socketChannelClass;
        eventLoopGroup = builder.eventLoopGroup == null ? new NioEventLoopGroup() : builder.eventLoopGroup;
        ownsEventLoopGroup = builder.eventLoopGroup == null;
        sslContext = builder.sslContext;
        inetAddressResolver = builder.inetAddressResolver;
    }
}
