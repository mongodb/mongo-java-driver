/*
 * Copyright 2015 MongoDB, Inc.
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
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A {@code StreamFactoryFactory} implementation for <a href='http://netty.io/'>Netty</a>-based streams.
 *
 * @since 3.1
 */
public class NettyStreamFactoryFactory implements StreamFactoryFactory {

    private final EventLoopGroup eventLoopGroup;
    private final Class<? extends SocketChannel> socketChannelClass;
    private final ByteBufAllocator allocator;

    /**
     * Construct an instance with the default {@code EventLoopGroup} and {@code ByteBufAllocator}.
     *
     * @deprecated Use {@link NettyStreamFactoryFactory#builder()} instead to construct the {@code  NettyStreamFactoryFactory}.
     */
    @Deprecated
    public NettyStreamFactoryFactory() {
        this(builder());
    }

    /**
     * Construct an instance with the given {@code EventLoopGroup} and {@code ByteBufAllocator}.
     *
     * @param eventLoopGroup the non-null event loop group
     * @param allocator the non-null byte buf allocator
     * @deprecated Use {@link NettyStreamFactoryFactory#builder()} instead to construct the {@code  NettyStreamFactoryFactory}.
     */
    @Deprecated
    public NettyStreamFactoryFactory(final EventLoopGroup eventLoopGroup, final ByteBufAllocator allocator) {
        this(builder().eventLoopGroup(eventLoopGroup).allocator(allocator));
    }

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
         * @param eventLoopGroup the event loop group that all channels created by this factory will be a part of
         * @return this
         */
        public Builder eventLoopGroup(final EventLoopGroup eventLoopGroup) {
            this.eventLoopGroup = notNull("eventLoopGroup", eventLoopGroup);
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
        return new NettyStreamFactory(socketSettings, sslSettings, eventLoopGroup, socketChannelClass, allocator);
    }

    @Override
    public String toString() {
        return "NettyStreamFactoryFactory{"
                + "eventLoopGroup=" + eventLoopGroup
                + ", socketChannelClass=" + socketChannelClass
                + ", allocator=" + allocator
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
    }
}
