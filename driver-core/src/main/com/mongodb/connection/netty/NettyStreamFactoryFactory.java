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

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A {@code StreamFactoryFactory} implementation for <a href='http://netty.io/'>Netty</a>-based streams.
 *
 * @since 3.1
 */
public class NettyStreamFactoryFactory implements StreamFactoryFactory {

    private final EventLoopGroup eventLoopGroup;
    private final ByteBufAllocator allocator;

    /**
     * Construct an instance with the given {@code EventLoopGroup} and {@code ByteBufAllocator}.
     *
     * @param eventLoopGroup the non-null event loop group
     * @param allocator the non-null byte buf allocator
     */
    public NettyStreamFactoryFactory(final EventLoopGroup eventLoopGroup, final ByteBufAllocator allocator) {
        this.eventLoopGroup = notNull("eventLoopGroup", eventLoopGroup);
        this.allocator = notNull("allocator", allocator);
    }

    /**
     * Construct an instance with the default {@code EventLoopGroup} and {@code ByteBufAllocator}.
     */
    public NettyStreamFactoryFactory() {
        this(new NioEventLoopGroup(), ByteBufAllocator.DEFAULT);
    }

    @Override
    public StreamFactory create(final SocketSettings socketSettings, final SslSettings sslSettings) {
        return new NettyStreamFactory(socketSettings, sslSettings, eventLoopGroup, allocator);
    }
}
