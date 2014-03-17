/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package org.mongodb.connection.netty;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.mongodb.connection.SSLSettings;
import org.mongodb.connection.ServerAddress;
import org.mongodb.connection.SocketSettings;
import org.mongodb.connection.Stream;
import org.mongodb.connection.StreamFactory;

import static org.mongodb.assertions.Assertions.notNull;

/**
 * A StreamFactory for Streams based on Netty 4.0
 *
 * @since 3.0
 */
public class NettyStreamFactory implements StreamFactory {
    private final SocketSettings settings;
    private final SSLSettings sslSettings;
    private final EventLoopGroup eventLoopGroup;

    public NettyStreamFactory(final SocketSettings settings, final SSLSettings sslSettings) {
        this.settings = notNull("settings", settings);
        this.sslSettings = notNull("sslSettings", sslSettings);
        this.eventLoopGroup = new NioEventLoopGroup();
    }

    @Override
    public Stream create(final ServerAddress serverAddress) {
        return new NettyStream(serverAddress, settings, sslSettings, eventLoopGroup);
    }
}
