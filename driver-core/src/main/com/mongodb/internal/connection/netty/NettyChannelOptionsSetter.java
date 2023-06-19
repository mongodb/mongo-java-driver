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

package com.mongodb.internal.connection.netty;

import com.mongodb.connection.SocketSettings;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelOption;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@FunctionalInterface
public interface NettyChannelOptionsSetter {
    <T> void set(ChannelOption<T> option, T value);

    static <T> void configureNettyChannelOptions(
            final SocketSettings socketSettings,
            final ByteBufAllocator allocator,
            final NettyChannelOptionsSetter optionSetter) {
        optionSetter.set(ChannelOption.CONNECT_TIMEOUT_MILLIS, socketSettings.getConnectTimeout(MILLISECONDS));
        optionSetter.set(ChannelOption.TCP_NODELAY, true);
        optionSetter.set(ChannelOption.SO_KEEPALIVE, true);
        if (socketSettings.getReceiveBufferSize() > 0) {
            optionSetter.set(ChannelOption.SO_RCVBUF, socketSettings.getReceiveBufferSize());
        }
        if (socketSettings.getSendBufferSize() > 0) {
            optionSetter.set(ChannelOption.SO_SNDBUF, socketSettings.getSendBufferSize());
        }
        optionSetter.set(ChannelOption.ALLOCATOR, allocator);
    }
}
