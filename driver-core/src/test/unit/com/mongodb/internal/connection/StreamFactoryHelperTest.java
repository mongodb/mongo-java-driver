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

package com.mongodb.internal.connection;

import com.mongodb.connection.NettyTransportSettings;
import com.mongodb.connection.TransportSettings;
import com.mongodb.internal.connection.netty.NettyStreamFactoryFactory;
import com.mongodb.spi.dns.InetAddressResolver;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SuppressWarnings("deprecation")
class StreamFactoryHelperTest {

    @Test
    void streamFactoryFactoryIsDerivedFromTransportSettings() {
        InetAddressResolver inetAddressResolver = new DefaultInetAddressResolver();
        NettyTransportSettings nettyTransportSettings = TransportSettings.nettyBuilder()
                .eventLoopGroup(new NioEventLoopGroup())
                .allocator(PooledByteBufAllocator.DEFAULT)
                .socketChannelClass(io.netty.channel.socket.oio.OioSocketChannel.class)
                .build();
        assertEquals(NettyStreamFactoryFactory.builder().applySettings(nettyTransportSettings)
                .inetAddressResolver(inetAddressResolver).build(),
                StreamFactoryHelper.getStreamFactoryFactoryFromSettings(nettyTransportSettings, inetAddressResolver));
    }
}
