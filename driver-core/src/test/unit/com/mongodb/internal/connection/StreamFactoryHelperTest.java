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

import com.mongodb.MongoClientSettings;
import com.mongodb.connection.NettyTransportSettings;
import com.mongodb.connection.TransportSettings;
import com.mongodb.connection.netty.NettyStreamFactoryFactory;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.oio.OioSocketChannel;
import org.junit.jupiter.api.Test;

import static com.mongodb.assertions.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SuppressWarnings("deprecation")
class StreamFactoryHelperTest {

    @Test
    void streamFactoryFactoryIsNullWithDefaultSettings() {
        MongoClientSettings settings = MongoClientSettings.builder().build();
        assertNull(StreamFactoryHelper.getStreamFactoryFactoryFromSettings(settings));
    }

    @Test
    void streamFactoryFactoryIsEqualToSettingsStreamFactoryFactory() {
        NettyStreamFactoryFactory streamFactoryFactory = NettyStreamFactoryFactory.builder().build();
        MongoClientSettings settings = MongoClientSettings.builder()
                .streamFactoryFactory(streamFactoryFactory)
                .build();
        assertEquals(streamFactoryFactory, StreamFactoryHelper.getStreamFactoryFactoryFromSettings(settings));
    }

    @Test
    void streamFactoryFactoryIsDerivedFromTransportSettings() {
        NettyTransportSettings nettyTransportSettings = TransportSettings.nettyBuilder()
                .eventLoopGroup(new NioEventLoopGroup())
                .allocator(PooledByteBufAllocator.DEFAULT)
                .socketChannelClass(OioSocketChannel.class)
                .build();
        MongoClientSettings settings = MongoClientSettings.builder()
                .transportSettings(nettyTransportSettings)
                .build();
        assertEquals(NettyStreamFactoryFactory.builder().applySettings(nettyTransportSettings).build(),
                StreamFactoryHelper.getStreamFactoryFactoryFromSettings(settings));
    }
}
