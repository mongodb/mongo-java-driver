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

import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class NettyTransportSettingsTest {
    @Test
    public void shouldDefaultAllValuesToNull() {
        NettyTransportSettings settings = TransportSettings.nettyBuilder().build();

        assertNull(settings.getAllocator());
        assertNull(settings.getEventLoopGroup());
        assertNull(settings.getSslContext());
        assertNull(settings.getSocketChannelClass());
    }

    @Test
    public void shouldApplySettingsFromBuilder() throws SSLException {
        EventLoopGroup eventLoopGroup = new DefaultEventLoopGroup();
        SslContext sslContext = SslContextBuilder.forClient().build();
        NettyTransportSettings settings = TransportSettings.nettyBuilder()
                .allocator(UnpooledByteBufAllocator.DEFAULT)
                .socketChannelClass(NioSocketChannel.class)
                .eventLoopGroup(eventLoopGroup)
                .sslContext(sslContext)
                .build();

        assertEquals(UnpooledByteBufAllocator.DEFAULT, settings.getAllocator());
        assertEquals(NioSocketChannel.class, settings.getSocketChannelClass());
        assertEquals(eventLoopGroup, settings.getEventLoopGroup());
        assertEquals(sslContext, settings.getSslContext());
    }
}
