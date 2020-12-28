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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.isTrueArgument;

/**
 * Passes a {@link ReadTimeoutException} if the time between a {@link #scheduleTimeout} and returned {@link TimeoutHandle#cancel()}
 * is longer than the set timeout.
 */
final class ReadTimeoutHandler extends ChannelInboundHandlerAdapter {
    private final long readTimeout;

    ReadTimeoutHandler(final long readTimeout) {
        isTrueArgument("readTimeout must be greater than zero.", readTimeout > 0);
        this.readTimeout = readTimeout;
    }

    TimeoutHandle scheduleTimeout(final ChannelHandlerContext ctx, final int additionalTimeout) {
        final SimpleTimeoutHandle timeoutHandle = new SimpleTimeoutHandle();
        scheduleTimeout(timeoutHandle, ctx, additionalTimeout);
        return timeoutHandle;
    }

    TimeoutHandle scheduleTimeout(final ExecutorService executor, final ChannelHandlerContext ctx, final int additionalTimeout) {
        final SimpleTimeoutHandle timeoutHandle = new SimpleTimeoutHandle();
        executor.submit(() -> scheduleTimeout(timeoutHandle, ctx, additionalTimeout));
        return timeoutHandle;
    }

    private void scheduleTimeout(final SimpleTimeoutHandle timeoutHandle, final ChannelHandlerContext ctx, final int additionalTimeout) {
        isTrue("Handler called from the eventLoop", ctx.channel().eventLoop().inEventLoop());

        final ReadTimeoutTask task = new ReadTimeoutTask(timeoutHandle, ctx);
        final ScheduledFuture<?> timeout = ctx.executor().schedule(task, readTimeout + additionalTimeout, TimeUnit.MILLISECONDS);
        timeoutHandle.assignTimeout(timeout);
    }

    private static final class SimpleTimeoutHandle implements TimeoutHandle {
        private AtomicBoolean cancelled = new AtomicBoolean(false);
        private ScheduledFuture<?> timeout = null;

        @Override
        public void cancel() {
            cancelled.set(true);
            if (timeout != null) {
                timeout.cancel(false);
            }
        }

        private boolean isCancelled() {
            return cancelled.get();
        }

        private void assignTimeout(final ScheduledFuture<?> timeout) {
            this.timeout = timeout;
        }
    }

    private static final class ReadTimeoutTask implements Runnable {

        private final SimpleTimeoutHandle timeoutHandle;
        private final ChannelHandlerContext ctx;

        ReadTimeoutTask(final SimpleTimeoutHandle timeoutHandle, final ChannelHandlerContext ctx) {
            this.timeoutHandle = timeoutHandle;
            this.ctx = ctx;
        }

        @Override
        public void run() {
            if (!timeoutHandle.isCancelled() && ctx.channel().isOpen()) {
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
