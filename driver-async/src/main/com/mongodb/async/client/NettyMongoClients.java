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

package com.mongodb.async.client;

import com.mongodb.MongoDriverInformation;
import com.mongodb.connection.StreamFactory;
import com.mongodb.connection.netty.NettyStreamFactory;
import com.mongodb.lang.Nullable;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.io.Closeable;

// Creation of MongoClient using NettyStreamFactory is segregated here to avoid a runtime dependency on Netty in MongoClients
final class NettyMongoClients {
    static MongoClient create(final MongoClientSettings settings, @Nullable final MongoDriverInformation mongoDriverInformation) {
        final EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
        StreamFactory streamFactory = new NettyStreamFactory(settings.getSocketSettings(), settings.getSslSettings(), eventLoopGroup);
        StreamFactory heartbeatStreamFactory = new NettyStreamFactory(settings.getHeartbeatSocketSettings(), settings.getSslSettings(),
                                                                             eventLoopGroup);
        return MongoClients.createMongoClient(settings, mongoDriverInformation, streamFactory, heartbeatStreamFactory,
                new Closeable() {
                    @Override
                    public void close() {
                        eventLoopGroup.shutdownGracefully().awaitUninterruptibly();
                    }
                });
    }

    private NettyMongoClients() {
    }
}
