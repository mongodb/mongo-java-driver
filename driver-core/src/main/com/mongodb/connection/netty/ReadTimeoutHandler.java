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

import com.mongodb.annotations.NotThreadSafe;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.lang.Nullable;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.handler.timeout.ReadTimeoutException;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.StampedLock;

import static com.mongodb.assertions.Assertions.isTrueArgument;

/**
 * This {@link ChannelInboundHandler} allows {@linkplain #scheduleTimeout(int) scheduling} and {@linkplain #removeTimeout() removing}
 * timeouts. A timeout is a delayed task that {@linkplain ChannelHandlerContext#fireExceptionCaught(Throwable) fires}
 * {@link ReadTimeoutException#INSTANCE} and {@linkplain ChannelHandlerContext#close() closes} the channel;
 * this can be prevented by removing the timeout.
 * <p>
 * This class guarantees that there are no concurrent timeouts scheduled for a channel.
 * Note that despite instances of this class are not thread-safe
 * (only {@linkplain io.netty.channel.ChannelHandler.Sharable sharable} handlers must be thread-safe),
 * methods {@link #scheduleTimeout(int)} and {@link #removeTimeout()} are linearizable.
 * <p>
 * The Netty-related lifecycle management in this class is inspired by the {@link IdleStateHandler}.
 * See the <a href="https://netty.io/wiki/new-and-noteworthy-in-4.0.html#simplified-channel-state-model">channel state model</a>
 * for additional details.
 */
@NotThreadSafe
final class ReadTimeoutHandler extends ChannelInboundHandlerAdapter {
    private final long readTimeoutMillis;
    private final Lock nonreentrantLock;
    @Nullable
    private ChannelHandlerContext ctx;
    @Nullable
    private ScheduledFuture<?> timeout;

    ReadTimeoutHandler(final long readTimeoutMillis) {
        isTrueArgument("readTimeoutMillis must be positive", readTimeoutMillis > 0);
        this.readTimeoutMillis = readTimeoutMillis;
        nonreentrantLock = new StampedLock().asWriteLock();
    }

    private void register(final ChannelHandlerContext context) {
        nonreentrantLock.lock();
        try {
            final ChannelHandlerContext ctx = this.ctx;
            if (ctx == context) {
                return;
            }
            assert ctx == null : "Attempted to register a context before a previous one is deregistered";
            this.ctx = context;
        } finally {
            nonreentrantLock.unlock();
        }
    }

    private void deregister() {
        nonreentrantLock.lock();
        try {
            unsynchronizedCancel();
            ctx = null;
        } finally {
            nonreentrantLock.unlock();
        }
    }

    private void unsynchronizedCancel() {
        final ScheduledFuture<?> timeout = this.timeout;
        if (timeout != null) {
            timeout.cancel(false);
            this.timeout = null;
        }
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        /* This method is invoked only if the handler is added to a channel pipeline before the channelActive event is fired.
         * Because of this fact we also need to monitor the handlerAdded event.*/
        register(ctx);
        super.channelActive(ctx);
    }

    @Override
    public void handlerAdded(final ChannelHandlerContext ctx) throws Exception {
        final Channel channel = ctx.channel();
        if (channel.isActive()//the channelActive event has already been fired and our channelActive method will not be called
                /* Check that the channel is registered with an event loop.
                 * If it is not the case, then our channelRegistered method calls the register method.*/
                && channel.isRegistered()) {
            register(ctx);
        } else {
            /* The channelActive event has not been fired. When it is fired, our channelActive method will be called
             * and we will call the register method there.*/
        }
        super.handlerAdded(ctx);
    }

    @Override
    public void channelRegistered(final ChannelHandlerContext ctx) throws Exception {
        if (ctx.channel().isActive()) {//the channelActive event has already been fired and our channelActive method will not be called
            register(ctx);
        }
        super.channelRegistered(ctx);
    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
        deregister();
        super.channelInactive(ctx);
    }

    @Override
    public void handlerRemoved(final ChannelHandlerContext ctx) throws Exception {
        deregister();
        super.handlerRemoved(ctx);
    }

    @Override
    public void channelUnregistered(final ChannelHandlerContext ctx) throws Exception {
        deregister();
        super.channelUnregistered(ctx);
    }

    /**
     * Schedules a new timeout.
     * A timeout must be {@linkplain #removeTimeout() removed} before another one is allowed to be scheduled.
     */
    @ThreadSafe
    void scheduleTimeout(final int additionalTimeoutMillis) {
        isTrueArgument("additionalTimeoutMillis must not be negative", additionalTimeoutMillis >= 0);
        nonreentrantLock.lock();
        try {
            final ChannelHandlerContext ctx = this.ctx;
            if (ctx == null) {//no context is registered
                return;
            }
            final ScheduledFuture<?> timeout = this.timeout;
            assert timeout == null || timeout.isDone() : "Attempted to schedule a timeout before the previous one is removed or completed";
            this.timeout = ctx.executor().schedule(() -> {
                try {
                    fireTimeoutException(ctx);
                } catch (final Throwable t) {
                    ctx.fireExceptionCaught(t);
                }
            }, readTimeoutMillis + additionalTimeoutMillis, TimeUnit.MILLISECONDS);
        } finally {
            nonreentrantLock.unlock();
        }
    }

    /**
     * Either removes the previously {@linkplain #scheduleTimeout(int) scheduled} timeout, or does nothing.
     * After removing a timeout, another one may be scheduled.
     */
    @ThreadSafe
    void removeTimeout() {
        nonreentrantLock.lock();
        try {
            unsynchronizedCancel();
        } finally {
            nonreentrantLock.unlock();
        }
    }

    private static void fireTimeoutException(final ChannelHandlerContext ctx) {
        if (!ctx.channel().isOpen()) {
            return;
        }
        ctx.fireExceptionCaught(ReadTimeoutException.INSTANCE);
        ctx.close();
    }
}
