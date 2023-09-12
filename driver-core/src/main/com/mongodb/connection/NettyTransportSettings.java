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

package com.mongodb.connection;

import com.mongodb.annotations.Immutable;
import com.mongodb.lang.Nullable;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.ReferenceCountedOpenSslClientContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;

import java.security.Security;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;

/**
 * {@code TransportSettings} for a <a href="http://netty.io/">Netty</a>-based transport implementation.
 *
 * @since 4.11
 */
@Immutable
public final class NettyTransportSettings extends TransportSettings {

    private final EventLoopGroup eventLoopGroup;
    private final Class<? extends SocketChannel> socketChannelClass;
    private final ByteBufAllocator allocator;
    private final SslContext sslContext;

    /**
     * Gets a builder for an instance of {@code NettyStreamFactoryFactory}.
     * @return the builder
     */
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder for an instance of {@link NettyTransportSettings}.
     */
    public static final class Builder {
        private ByteBufAllocator allocator;
        private Class<? extends SocketChannel> socketChannelClass;
        private EventLoopGroup eventLoopGroup;
        private SslContext sslContext;

        private Builder() {
        }

        /**
         * Sets the allocator.
         *
         * @param allocator the allocator to use for ByteBuf instances
         * @return this
         * @see #getAllocator()
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
         * @see #getSocketChannelClass()
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
         * @see #getEventLoopGroup()
         */
        public Builder eventLoopGroup(final EventLoopGroup eventLoopGroup) {
            this.eventLoopGroup = notNull("eventLoopGroup", eventLoopGroup);
            return this;
        }

        /**
         * Sets a {@linkplain SslContextBuilder#forClient() client-side} {@link SslContext io.netty.handler.ssl.SslContext},
         * which overrides the standard {@link SslSettings#getContext()}.
         * By default, it is {@code null} and {@link SslSettings#getContext()} is at play.
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
         * @see #getSslContext()
         */
        public Builder sslContext(final SslContext sslContext) {
            this.sslContext = notNull("sslContext", sslContext);
            isTrueArgument("sslContext must be client-side", sslContext.isClient());
            isTrueArgument("sslContext must use either SslProvider.JDK or SslProvider.OPENSSL TLS/SSL protocol provider",
                    !(sslContext instanceof ReferenceCountedOpenSslClientContext));

            return this;
        }

        /**
         * Build an instance of {@code NettyStreamFactoryFactory}.
         * @return factory of the netty stream factory
         */
        public NettyTransportSettings build() {
            return new NettyTransportSettings(this);
        }
    }

    /**
     * Gets the event loop group.
     *
     * @return the event loop group
     * @see Builder#eventLoopGroup(EventLoopGroup)
     */
    @Nullable
    public EventLoopGroup getEventLoopGroup() {
        return eventLoopGroup;
    }

    /**
     * Gets the socket channel class.
     *
     * @return the socket channel class
     * @see Builder#socketChannelClass(Class)
     */
    @Nullable
    public Class<? extends SocketChannel> getSocketChannelClass() {
        return socketChannelClass;
    }

    /**
     * Gets the allocator.
     *
     * @return the allocator
     * @see Builder#allocator(ByteBufAllocator)
     */
    @Nullable
    public ByteBufAllocator getAllocator() {
        return allocator;
    }

    /**
     * Gets the SSL Context.
     *
     * @return the SSL context
     * @see Builder#sslContext(SslContext)
     */
    @Nullable
    public SslContext getSslContext() {
        return sslContext;
    }

    @Override
    public String toString() {
        return "NettyTransportSettings{"
                + "eventLoopGroup=" + eventLoopGroup
                + ", socketChannelClass=" + socketChannelClass
                + ", allocator=" + allocator
                + ", sslContext=" + sslContext
                + '}';
    }

    private NettyTransportSettings(final Builder builder) {
        allocator = builder.allocator;
        socketChannelClass = builder.socketChannelClass;
        eventLoopGroup = builder.eventLoopGroup;
        sslContext = builder.sslContext;
    }
}
