/*
 * Copyright 2008-present MongoDB, Inc.
 * Copyright 2012 The Netty Project
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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.ReadTimeoutException;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.isTrueArgument;

/**
 * Passes a {@link ReadTimeoutException} if the time between a {@link #scheduleTimeout} and {@link #removeTimeout} is longer than the set
 * timeout.
 */
final class ReadTimeoutHandler extends ChannelInboundHandlerAdapter {
    private final long readTimeout;
    private volatile ScheduledFuture<?> timeout;

    ReadTimeoutHandler(final long readTimeout) {
        isTrueArgument("readTimeout must be greater than zero.", readTimeout > 0);
        this.readTimeout = readTimeout;
    }

    void scheduleTimeout(final ChannelHandlerContext ctx) {
        isTrue("Handler called from the eventLoop", ctx.channel().eventLoop().inEventLoop());
        if (timeout == null) {
            timeout = ctx.executor().schedule(new ReadTimeoutTask(ctx), readTimeout, TimeUnit.MILLISECONDS);
        }
    }

    void removeTimeout(final ChannelHandlerContext ctx) {
        isTrue("Handler called from the eventLoop", ctx.channel().eventLoop().inEventLoop());
        if (timeout != null) {
            timeout.cancel(false);
            timeout = null;
        }
    }

    private static final class ReadTimeoutTask implements Runnable {

        private final ChannelHandlerContext ctx;

        ReadTimeoutTask(final ChannelHandlerContext ctx) {
            this.ctx = ctx;
        }

        @Override
        public void run() {
            if (ctx.channel().isOpen()) {
                try {
                    ctx.fireExceptionCaught(ReadTimeoutException.INSTANCE);
                    ctx.close();
                } catch (Throwable t) {
                    ctx.fireExceptionCaught(t);
                }
            }
        }
    }
}
