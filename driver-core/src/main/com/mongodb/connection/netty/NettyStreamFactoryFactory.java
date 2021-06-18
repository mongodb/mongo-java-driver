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

package com.mongodb.connection.netty;

import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.connection.StreamFactory;
import com.mongodb.connection.StreamFactoryFactory;
import com.mongodb.lang.Nullable;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;

import java.security.Security;
import java.util.function.Consumer;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A {@code StreamFactoryFactory} implementation for <a href='http://netty.io/'>Netty</a>-based streams.
 *
 * @since 3.1
 */
public final class NettyStreamFactoryFactory implements StreamFactoryFactory {

    private final EventLoopGroup eventLoopGroup;
    private final Class<? extends SocketChannel> socketChannelClass;
    private final ByteBufAllocator allocator;
    @Nullable
    private final Consumer<? super SslContextBuilder> nettySslContextTuner;

    /**
     * Gets a builder for an instance of {@code NettyStreamFactoryFactory}.
     * @return the builder
     * @since 3.3
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * A builder for an instance of {@code NettyStreamFactoryFactory}.
     *
     * @since 3.3
     */
    public static final class Builder {
        private ByteBufAllocator allocator;
        private Class<? extends SocketChannel> socketChannelClass;
        private EventLoopGroup eventLoopGroup;
        @Nullable
        private Consumer<? super SslContextBuilder> nettySslContextTuner;

        private Builder() {
            allocator(ByteBufAllocator.DEFAULT);
            socketChannelClass(NioSocketChannel.class);
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
         * Sets the tuner for a {@linkplain SslContextBuilder#forClient() client-side} {@link SslContext io.netty.handler.ssl.SslContext},
         * which overrides the standard {@link SslSettings#getContext()}.
         * By default the tuner is {@code null} and {@link SslSettings#getContext()} is at play.
         * <p>
         * This option may be used as a convenient way to utilize
         * <a href="https://www.openssl.org/">OpenSSL</a> as an alternative to the TLS/SSL protocol implementation in a JDK.
         * To achieve this, specify {@link SslProvider#OPENSSL}
         * {@linkplain SslContextBuilder#sslProvider(SslProvider) TLS/SSL protocol provider} via the tuner.
         * Note that doing so adds a runtime dependency on
         * <a href="https://netty.io/wiki/forked-tomcat-native.html">netty-tcnative</a>, which you must satisfy.
         * <p>
         * Notes:
         * <ul>
         *    <li>Netty {@link SslContext} may not examine some
         *    {@linkplain Security security}/{@linkplain System#getProperties() system} properties that are used to
         *    <a href="https://docs.oracle.com/javase/8/docs/technotes/guides/security/jsse/JSSERefGuide.html#Customization">
         *    customize JSSE</a>. Therefore, instead of using them you may have to apply the equivalent configuration programmatically
         *    via the tuner, if both the {@link SslContextBuilder} and the TLS/SSL protocol provider of choice support it.
         *    </li>
         *    <li>Only {@link SslProvider#JDK} and {@link SslProvider#OPENSSL} TLS/SSL protocol providers are supported.
         *    </li>
         * </ul>
         *
         * @param nettySslContextTuner The tuner for an {@link SslContext}.
         *                             The {@link SslContextBuilder} {@linkplain Consumer#accept(Object) supplied} to the tuner
         *                             is created via {@linkplain SslContextBuilder#forClient()}.
         * @return {@code this}.
         *
         * @since 4.3
         */
        public Builder applyToNettySslContext(final Consumer<? super SslContextBuilder> nettySslContextTuner) {
            this.nettySslContextTuner = notNull("nettySslContextTuner", nettySslContextTuner);
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
        return new NettyStreamFactory(socketSettings, sslSettings, eventLoopGroup, socketChannelClass, allocator, nettySslContextTuner);
    }

    @Override
    public String toString() {
        return "NettyStreamFactoryFactory{"
                + "eventLoopGroup=" + eventLoopGroup
                + ", socketChannelClass=" + socketChannelClass
                + ", allocator=" + allocator
                + ", nettySslContextTuner=" + nettySslContextTuner
                + '}';
    }

    private NettyStreamFactoryFactory(final Builder builder) {
        allocator = builder.allocator;
        socketChannelClass = builder.socketChannelClass;
        if (builder.eventLoopGroup != null) {
            eventLoopGroup = builder.eventLoopGroup;
        } else {
            eventLoopGroup = new NioEventLoopGroup();
        }
        nettySslContextTuner = builder.nettySslContextTuner;
    }
}
