/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.mongodb.connection.netty

import com.mongodb.ClusterFixture
import com.mongodb.connection.SocketSettings
import com.mongodb.connection.SslSettings
import io.netty.buffer.PooledByteBufAllocator
import io.netty.buffer.UnpooledByteBufAllocator
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.oio.OioEventLoopGroup
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.channel.socket.oio.OioSocketChannel
import spock.lang.Specification

class NettyStreamFactorySpecification extends Specification {

    private static final SOCKET_SETTINGS = SocketSettings.builder().keepAlive(true).build()
    private static final SSL_SETTINGS = SslSettings.builder().enabled(true).invalidHostNameAllowed(true).build()
    private static final EVENT_LOOP_GROUP = new OioEventLoopGroup()
    private static final SOCKET_CHANNEL_CLASS = OioSocketChannel
    private static final ALLOCATOR = UnpooledByteBufAllocator.DEFAULT

    def cleanupSpec() {
        EVENT_LOOP_GROUP.shutdownGracefully().awaitUninterruptibly()
    }

    def 'should use arguments to create NettyStream'() {
        when:
        NettyStream stream = factory.create(ClusterFixture.getPrimary()) as NettyStream

        then:
        stream.address == ClusterFixture.getPrimary()
        stream.settings == SOCKET_SETTINGS
        stream.sslSettings == SSL_SETTINGS
        stream.socketChannelClass == socketChannelClass
        stream.getWorkerGroup().class == eventLoopGroupClass
        stream.allocator == allocator

        cleanup:
        stream?.close()
        if (stream.getWorkerGroup().class != eventLoopGroupClass) {
            stream.getWorkerGroup().shutdownGracefully().awaitUninterruptibly()
        }

        where:
        eventLoopGroupClass  | socketChannelClass   | allocator                      | factory
        NioEventLoopGroup    | NioSocketChannel     | PooledByteBufAllocator.DEFAULT | new NettyStreamFactory(SOCKET_SETTINGS, SSL_SETTINGS)
        OioEventLoopGroup    | NioSocketChannel     | PooledByteBufAllocator.DEFAULT | new NettyStreamFactory(SOCKET_SETTINGS, SSL_SETTINGS,
                EVENT_LOOP_GROUP)
        OioEventLoopGroup    | NioSocketChannel     | ALLOCATOR                      | new NettyStreamFactory(SOCKET_SETTINGS, SSL_SETTINGS,
                EVENT_LOOP_GROUP, ALLOCATOR)
        OioEventLoopGroup    | SOCKET_CHANNEL_CLASS | ALLOCATOR                      | new NettyStreamFactory(SOCKET_SETTINGS, SSL_SETTINGS,
                EVENT_LOOP_GROUP, SOCKET_CHANNEL_CLASS, ALLOCATOR)
    }
}
