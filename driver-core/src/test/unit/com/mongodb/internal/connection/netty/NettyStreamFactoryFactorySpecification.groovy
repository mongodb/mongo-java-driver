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

package com.mongodb.internal.connection.netty

import com.mongodb.ServerAddress
import com.mongodb.connection.SocketSettings
import com.mongodb.connection.SslSettings
import com.mongodb.connection.TransportSettings
import com.mongodb.internal.connection.DefaultInetAddressResolver
import io.netty.buffer.ByteBufAllocator
import io.netty.buffer.UnpooledByteBufAllocator
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.oio.OioEventLoopGroup
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.channel.socket.oio.OioSocketChannel
import io.netty.handler.ssl.SslContextBuilder
import spock.lang.Specification
import spock.lang.Unroll

class NettyStreamFactoryFactorySpecification extends Specification {

    def 'should apply NettingSettings'() {
        given:
        def nettySettings = TransportSettings.nettyBuilder()
                .allocator(UnpooledByteBufAllocator.DEFAULT)
                .socketChannelClass(OioSocketChannel)
                .eventLoopGroup(new OioEventLoopGroup())
                .sslContext(SslContextBuilder.forClient().build())
            .build()

        when:
        def factoryFactory = NettyStreamFactoryFactory.builder()
                .inetAddressResolver(new DefaultInetAddressResolver())
                .applySettings(nettySettings)
                .build()

        then:
        factoryFactory.getAllocator() == nettySettings.getAllocator()
        factoryFactory.getEventLoopGroup() == nettySettings.getEventLoopGroup();
        factoryFactory.getSocketChannelClass() == nettySettings.getSocketChannelClass()
        factoryFactory.getSslContext() == nettySettings.getSslContext()
    }

    @Unroll
    def 'should create the expected #description NettyStream'() {
        given:
        def factory = factoryFactory.create(socketSettings, sslSettings)

        when:
        NettyStream stream = factory.create(serverAddress)

        then:
        stream.getSettings() == socketSettings
        stream.getSslSettings() == sslSettings
        stream.getAddress() == serverAddress
        stream.getAllocator() == allocator
        stream.getSocketChannelClass() == socketChannelClass
        stream.getWorkerGroup().getClass() == eventLoopGroupClass

        where:
        description | factoryFactory  | allocator                         | socketChannelClass | eventLoopGroupClass
        'default'   | DEFAULT_FACTORY | ByteBufAllocator.DEFAULT          | NioSocketChannel   | NioEventLoopGroup
        'custom'    | CUSTOM_FACTORY  | UnpooledByteBufAllocator.DEFAULT  | OioSocketChannel   | OioEventLoopGroup
    }

    SocketSettings socketSettings = SocketSettings.builder().build()
    SslSettings sslSettings = SslSettings.builder().build()
    ServerAddress serverAddress = new ServerAddress()
    static final DEFAULT_FACTORY = NettyStreamFactoryFactory.builder()
            .inetAddressResolver(new DefaultInetAddressResolver())
            .build()
    static final CUSTOM_FACTORY = NettyStreamFactoryFactory.builder()
            .allocator(UnpooledByteBufAllocator.DEFAULT)
            .socketChannelClass(OioSocketChannel)
            .eventLoopGroup(new OioEventLoopGroup())
            .inetAddressResolver(new DefaultInetAddressResolver())
            .build()
}
