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
 *
 * Original Work: MIT License, Copyright (c) [2015-2018] all contributors
 * https://github.com/marianobarrios/tls-channel
 */

package com.mongodb.internal.connection.tlschannel.async;

import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.internal.connection.tlschannel.NeedsReadException;
import com.mongodb.internal.connection.tlschannel.NeedsTaskException;
import com.mongodb.internal.connection.tlschannel.NeedsWriteException;
import com.mongodb.internal.connection.tlschannel.TlsChannel;
import com.mongodb.internal.connection.tlschannel.impl.ByteBufferSet;
import com.mongodb.internal.connection.tlschannel.util.Util;

import java.io.IOException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.InterruptedByTimeoutException;
import java.nio.channels.ReadPendingException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ShutdownChannelGroupException;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritePendingException;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.IntBinaryOperator;
import java.util.function.LongConsumer;

import static java.lang.String.format;

/**
 * This class encapsulates the infrastructure for running {@link AsynchronousTlsChannel}s. Each instance of this class
 * is a singleton-like object that manages a thread pool that makes it possible to run a group of asynchronous
 * channels.
 */
public class AsynchronousTlsChannelGroup {

    private static final Logger LOGGER = Loggers.getLogger("connection.tls");

    /**
     * The main executor of the group has a queue, whose size is a multiple of the number of CPUs.
     */
    private static final int QUEUE_LENGTH_MULTIPLIER = 32;

    private static AtomicInteger globalGroupCount = new AtomicInteger();

    class RegisteredSocket {

        final TlsChannel tlsChannel;
        final SocketChannel socketChannel;

        /**
         * Used to wait until the channel is effectively in the selector (which happens asynchronously to the initial
         * registration.
         */
        final CountDownLatch registered = new CountDownLatch(1);

        SelectionKey key;

        /**
         * Protects {@link #readOperation} reference and instance.
         */
        final Lock readLock = new ReentrantLock();

        /**
         * Protects {@link #writeOperation} reference and instance.
         */
        final Lock writeLock = new ReentrantLock();

        /**
         * Current read operation, in not null
         */
        ReadOperation readOperation;

        /**
         * Current write operation, if not null
         */
        WriteOperation writeOperation;

        /**
         * Bitwise union of pending operation to be registered in the selector
         */
        final AtomicInteger pendingOps = new AtomicInteger();

        RegisteredSocket(final TlsChannel tlsChannel, final SocketChannel socketChannel) {
            this.tlsChannel = tlsChannel;
            this.socketChannel = socketChannel;
        }

        public void close() {
            doCancelRead(this, null);
            doCancelWrite(this, null);
            key.cancel();
            currentRegistrations.getAndDecrement();
            /*
             * Actual de-registration from the selector will happen asynchronously.
             */
            selector.wakeup();
        }
    }

    private abstract static class Operation {
        final ByteBufferSet bufferSet;
        final LongConsumer onSuccess;
        final Consumer<Throwable> onFailure;
        Future<?> timeoutFuture;

        Operation(final ByteBufferSet bufferSet, final LongConsumer onSuccess, final Consumer<Throwable> onFailure) {
            this.bufferSet = bufferSet;
            this.onSuccess = onSuccess;
            this.onFailure = onFailure;
        }
    }

    static final class ReadOperation extends Operation {
        ReadOperation(final ByteBufferSet bufferSet, final LongConsumer onSuccess, final Consumer<Throwable> onFailure) {
            super(bufferSet, onSuccess, onFailure);
        }
    }

    static final class WriteOperation extends Operation {

        /**
         * Because a write operation can flag a block (needs read/write) even after the source buffer was read from, we
         * need to accumulate consumed bytes.
         */
        long consumesBytes = 0;

        WriteOperation(final ByteBufferSet bufferSet, final LongConsumer onSuccess, final Consumer<Throwable> onFailure) {
            super(bufferSet, onSuccess, onFailure);
        }
    }

    private final int id = globalGroupCount.getAndIncrement();

    /**
     * With the intention of being spacer with warnings, use this flag to ensure that we only log the warning about
     * needed task once.
     */
    private final AtomicBoolean loggedTaskWarning = new AtomicBoolean();

    private final Selector selector;

    final ExecutorService executor;

    private final ScheduledThreadPoolExecutor timeoutExecutor = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
        @Override
        public Thread newThread(final Runnable runnable) {
            return new Thread(runnable, format("async-channel-group-%d-timeout-thread", id));
        }
    }
    );

    private final Thread selectorThread = new Thread(new Runnable() {
        @Override
        public void run() {
            AsynchronousTlsChannelGroup.this.loop();
        }
    }, format("async-channel-group-%d-selector", id));

    private final ConcurrentLinkedQueue<RegisteredSocket> pendingRegistrations = new ConcurrentLinkedQueue<RegisteredSocket>();

    private enum Shutdown {
        No, Wait, Immediate
    }

    private volatile Shutdown shutdown = Shutdown.No;

    private LongAdder selectionCount = new LongAdder();

    private LongAdder startedReads = new LongAdder();
    private LongAdder startedWrites = new LongAdder();
    private LongAdder successfulReads = new LongAdder();
    private LongAdder successfulWrites = new LongAdder();
    private LongAdder failedReads = new LongAdder();
    private LongAdder failedWrites = new LongAdder();
    private LongAdder cancelledReads = new LongAdder();
    private LongAdder cancelledWrites = new LongAdder();

    // used for synchronization
    private AtomicInteger currentRegistrations = new AtomicInteger();

    private LongAdder currentReads = new LongAdder();
    private LongAdder currentWrites = new LongAdder();

    /**
     * Creates an instance of this class.
     *
     * @param nThreads number of threads in the executor used to assist the selector loop and run completion handlers.
     */
    public AsynchronousTlsChannelGroup(final int nThreads) {
        try {
            selector = Selector.open();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        timeoutExecutor.setRemoveOnCancelPolicy(true);
        this.executor = new ThreadPoolExecutor(
                nThreads, nThreads,
                0, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(nThreads * QUEUE_LENGTH_MULTIPLIER),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(final Runnable runnable) {
                        return new Thread(runnable, format("async-channel-group-%d-handler-executor", id));
                    }
                },
                new ThreadPoolExecutor.CallerRunsPolicy());
        selectorThread.start();
    }

    /**
     * Creates an instance of this class, using as many thread as available processors.
     */
    public AsynchronousTlsChannelGroup() {
        this(Runtime.getRuntime().availableProcessors());
    }

    RegisteredSocket registerSocket(final TlsChannel reader, final SocketChannel socketChannel) {
        if (shutdown != Shutdown.No) {
            throw new ShutdownChannelGroupException();
        }
        RegisteredSocket socket = new RegisteredSocket(reader, socketChannel);
        currentRegistrations.getAndIncrement();
        pendingRegistrations.add(socket);
        selector.wakeup();
        return socket;
    }

    boolean doCancelRead(final RegisteredSocket socket, final ReadOperation op) {
        socket.readLock.lock();
        try {
            // a null op means cancel any operation
            if (op != null && socket.readOperation == op || op == null && socket.readOperation != null) {
                socket.readOperation = null;
                cancelledReads.increment();
                currentReads.decrement();
                return true;
            } else {
                return false;
            }
        } finally {
            socket.readLock.unlock();
        }
    }

    boolean doCancelWrite(final RegisteredSocket socket, final WriteOperation op) {
        socket.writeLock.lock();
        try {
            // a null op means cancel any operation
            if (op != null && socket.writeOperation == op || op == null && socket.writeOperation != null) {
                socket.writeOperation = null;
                cancelledWrites.increment();
                currentWrites.decrement();
                return true;
            } else {
                return false;
            }
        } finally {
            socket.writeLock.unlock();
        }
    }

    ReadOperation startRead(
            final RegisteredSocket socket,
            final ByteBufferSet buffer,
            final long timeout, final TimeUnit unit,
            final LongConsumer onSuccess, final Consumer<Throwable> onFailure)
            throws ReadPendingException {
        checkTerminated();
        Util.assertTrue(buffer.hasRemaining());
        waitForSocketRegistration(socket);
        ReadOperation op;
        socket.readLock.lock();
        try {
            if (socket.readOperation != null) {
                throw new ReadPendingException();
            }
            op = new ReadOperation(buffer, onSuccess, onFailure);
            final ReadOperation finalOp = op;
            /*
             * we do not try to outsmart the TLS state machine and register for both IO operations for each new socket
             * operation
             */
            socket.pendingOps.set(SelectionKey.OP_WRITE | SelectionKey.OP_READ);
            if (timeout != 0) {
                op.timeoutFuture = timeoutExecutor.schedule(new Runnable() {
                    @Override
                    public void run() {
                        boolean success = AsynchronousTlsChannelGroup.this.doCancelRead(socket, finalOp);
                        if (success) {
                            finalOp.onFailure.accept(new InterruptedByTimeoutException());
                        }
                    }
                }, timeout, unit);
            }
            socket.readOperation = op;
        } finally {
            socket.readLock.unlock();
        }
        selector.wakeup();
        startedReads.increment();
        currentReads.increment();
        return op;
    }

    WriteOperation startWrite(
            final RegisteredSocket socket,
            final ByteBufferSet buffer,
            final long timeout, final TimeUnit unit,
            final LongConsumer onSuccess, final Consumer<Throwable> onFailure)
            throws WritePendingException {
        checkTerminated();
        Util.assertTrue(buffer.hasRemaining());
        waitForSocketRegistration(socket);
        WriteOperation op;
        socket.writeLock.lock();
        try {
            if (socket.writeOperation != null) {
                throw new WritePendingException();
            }
            op = new WriteOperation(buffer, onSuccess, onFailure);
            final WriteOperation finalOp = op;
            /*
             * we do not try to outsmart the TLS state machine and register for both IO operations for each new socket
             * operation
             */
            socket.pendingOps.set(SelectionKey.OP_WRITE | SelectionKey.OP_READ);
            if (timeout != 0) {
                op.timeoutFuture = timeoutExecutor.schedule(new Runnable() {
                    @Override
                    public void run() {
                        boolean success = AsynchronousTlsChannelGroup.this.doCancelWrite(socket, finalOp);
                        if (success) {
                            finalOp.onFailure.accept(new InterruptedByTimeoutException());
                        }
                    }
                }, timeout, unit);
            }
            socket.writeOperation = op;
        } finally {
            socket.writeLock.unlock();
        }
        selector.wakeup();
        startedWrites.increment();
        currentWrites.increment();
        return op;
    }

    private void checkTerminated() {
        if (isTerminated()) {
            throw new ShutdownChannelGroupException();
        }
    }

    private void waitForSocketRegistration(final RegisteredSocket socket) {
        try {
            socket.registered.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void loop() {
        try {
            while (shutdown == Shutdown.No || shutdown == Shutdown.Wait && currentRegistrations.intValue() > 0) {
                int c = selector.select(); // block
                selectionCount.increment();
                // avoid unnecessary creation of iterator object
                if (c > 0) {
                    Iterator<SelectionKey> it = selector.selectedKeys().iterator();
                    while (it.hasNext()) {
                        SelectionKey key = it.next();
                        it.remove();
                        try {
                            key.interestOps(0);
                        } catch (CancelledKeyException e) {
                            // can happen when channels are closed with pending operations
                            continue;
                        }
                        RegisteredSocket socket = (RegisteredSocket) key.attachment();
                        processRead(socket);
                        processWrite(socket);
                    }
                }
                registerPendingSockets();
                processPendingInterests();
            }
        } catch (Throwable e) {
            LOGGER.error("error in selector loop", e);
        } finally {
            executor.shutdown();
            // use shutdownNow to stop delayed tasks
            timeoutExecutor.shutdownNow();
            if (shutdown == Shutdown.Immediate) {
                for (SelectionKey key : selector.keys()) {
                    RegisteredSocket socket = (RegisteredSocket) key.attachment();
                    socket.close();
                }
            }
            try {
                selector.close();
            } catch (IOException e) {
                LOGGER.warn(format("error closing selector: %s", e.getMessage()));
            }
        }
    }

    private void processPendingInterests() {
        for (SelectionKey key : selector.keys()) {
            RegisteredSocket socket = (RegisteredSocket) key.attachment();
            int pending = socket.pendingOps.getAndSet(0);
            if (pending != 0) {
                key.interestOps(key.interestOps() | pending);
            }
        }
    }

    private void processWrite(final RegisteredSocket socket) {
        socket.writeLock.lock();
        try {
            final WriteOperation op = socket.writeOperation;
            if (op != null) {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            AsynchronousTlsChannelGroup.this.doWrite(socket, op);
                        } catch (Throwable e) {
                            LOGGER.error("error in operation", e);
                        }
                    }
                });
            }
        } finally {
            socket.writeLock.unlock();
        }
    }

    private void processRead(final RegisteredSocket socket) {
        socket.readLock.lock();
        try {
            final ReadOperation op = socket.readOperation;
            if (op != null) {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            AsynchronousTlsChannelGroup.this.doRead(socket, op);
                        } catch (Throwable e) {
                            LOGGER.error("error in operation", e);
                        }
                    }
                });
            }
        } finally {
            socket.readLock.unlock();
        }
    }

    private void doWrite(final RegisteredSocket socket, final WriteOperation op) {
        socket.writeLock.lock();
        try {
            if (socket.writeOperation != op) {
                return;
            }
            try {
                long before = op.bufferSet.remaining();
                try {
                    writeHandlingTasks(socket, op);
                } finally {
                    long c = before - op.bufferSet.remaining();
                    Util.assertTrue(c >= 0);
                    op.consumesBytes += c;
                }
                socket.writeOperation = null;
                if (op.timeoutFuture != null) {
                    op.timeoutFuture.cancel(false);
                }
                op.onSuccess.accept(op.consumesBytes);
                successfulWrites.increment();
                currentWrites.decrement();
            } catch (NeedsReadException e) {
                socket.pendingOps.accumulateAndGet(SelectionKey.OP_READ, new IntBinaryOperator() {
                    @Override
                    public int applyAsInt(final int a, final int b) {
                        return a | b;
                    }
                });
                selector.wakeup();
            } catch (NeedsWriteException e) {
                socket.pendingOps.accumulateAndGet(SelectionKey.OP_WRITE, new IntBinaryOperator() {
                    @Override
                    public int applyAsInt(final int a, final int b) {
                        return a | b;
                    }
                });
                selector.wakeup();
            } catch (IOException e) {
                if (socket.writeOperation == op) {
                    socket.writeOperation = null;
                }
                if (op.timeoutFuture != null) {
                    op.timeoutFuture.cancel(false);
                }
                op.onFailure.accept(e);
                failedWrites.increment();
                currentWrites.decrement();
            }
        } finally {
            socket.writeLock.unlock();
        }
    }

    /**
     * Intended use of the channel group is with sockets that run tasks internally, but out of tolerance, run tasks in
     * thread in case the socket does not.
     */
    private void writeHandlingTasks(final RegisteredSocket socket, final WriteOperation op) throws IOException {
        while (true) {
            try {
                socket.tlsChannel.write(op.bufferSet.array, op.bufferSet.offset, op.bufferSet.length);
                return;
            } catch (NeedsTaskException e) {
                warnAboutNeedTask();
                e.getTask().run();
            }
        }
    }

    private void warnAboutNeedTask() {
        if (!loggedTaskWarning.getAndSet(true)) {
            LOGGER.warn(format(
                    "caught %s; channels used in asynchronous groups should run tasks themselves; "
                            + "although task is being dealt with anyway, consider configuring channels properly",
                    NeedsTaskException.class.getName()));
        }
    }

    private void doRead(final RegisteredSocket socket, final ReadOperation op) {
        socket.readLock.lock();
        try {
            if (socket.readOperation != op) {
                return;
            }
            try {
                Util.assertTrue(op.bufferSet.hasRemaining());
                long c = readHandlingTasks(socket, op);
                Util.assertTrue(c > 0 || c == -1);
                socket.readOperation = null;
                if (op.timeoutFuture != null) {
                    op.timeoutFuture.cancel(false);
                }
                op.onSuccess.accept(c);
                successfulReads.increment();
                currentReads.decrement();
            } catch (NeedsReadException e) {
                socket.pendingOps.accumulateAndGet(SelectionKey.OP_READ, new IntBinaryOperator() {
                    @Override
                    public int applyAsInt(final int a, final int b) {
                        return a | b;
                    }
                });
                selector.wakeup();
            } catch (NeedsWriteException e) {
                socket.pendingOps.accumulateAndGet(SelectionKey.OP_WRITE, new IntBinaryOperator() {
                    @Override
                    public int applyAsInt(final int a, final int b) {
                        return a | b;
                    }
                });
                selector.wakeup();
            } catch (IOException e) {
                if (socket.readOperation == op) {
                    socket.readOperation = null;
                }
                if (op.timeoutFuture != null) {
                    op.timeoutFuture.cancel(false);
                }
                op.onFailure.accept(e);
                failedReads.increment();
                currentReads.decrement();
            }
        } finally {
            socket.readLock.unlock();
        }
    }

    /**
     * @see #writeHandlingTasks
     */
    private long readHandlingTasks(final RegisteredSocket socket, final ReadOperation op) throws IOException {
        while (true) {
            try {
                return socket.tlsChannel.read(op.bufferSet.array, op.bufferSet.offset, op.bufferSet.length);
            } catch (NeedsTaskException e) {
                warnAboutNeedTask();
                e.getTask().run();
            }
        }
    }

    private void registerPendingSockets() throws ClosedChannelException {
        RegisteredSocket socket;
        while ((socket = pendingRegistrations.poll()) != null) {
            socket.key = socket.socketChannel.register(selector, 0, socket);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(format("registered key: %ss", socket.key));
            }
            socket.registered.countDown();
        }
    }

    /**
     * Whether either {@link #shutdown()} or {@link #shutdownNow()} have been called.
     *
     * @return {@code true} if this group has initiated shutdown and {@code false} if the group is active
     */
    public boolean isShutdown() {
        return shutdown != Shutdown.No;
    }

    /**
     * Starts the shutdown process. New sockets cannot be registered, already registered one continue operating normally
     * until they are closed.
     */
    public void shutdown() {
        shutdown = Shutdown.Wait;
        selector.wakeup();
    }

    /**
     * Shuts down this channel group immediately. All registered sockets are closed, pending operations may or may not
     * finish.
     */
    public void shutdownNow() {
        shutdown = Shutdown.Immediate;
        selector.wakeup();
    }

    /**
     * Whether this channel group was shut down, and all pending tasks have drained.
     */
    public boolean isTerminated() {
        return executor.isTerminated();
    }

    /**
     * Blocks until all registers sockets are closed and pending tasks finished execution after a shutdown request, or
     * the timeout occurs, or the current thread is interrupted, whichever happens first.
     *
     * @param timeout the maximum time to wait
     * @param unit    the time unit of the timeout argument
     * @return {@code true} if this group terminated and {@code false} if the group elapsed before termination
     * @throws InterruptedException if interrupted while waiting
     */
    public boolean awaitTermination(final long timeout, final TimeUnit unit) throws InterruptedException {
        return executor.awaitTermination(timeout, unit);
    }

    long getSelectionCount() {
        return selectionCount.longValue();
    }

    /**
     * Return the total number of read operations that were started.
     *
     * @return number of operations
     */
    public long getStartedReadCount() {
        return startedReads.longValue();
    }

    /**
     * Return the total number of write operations that were started.
     *
     * @return number of operations
     */
    public long getStartedWriteCount() {
        return startedWrites.longValue();
    }

    /**
     * Return the total number of read operations that succeeded.
     *
     * @return number of operations
     */
    public long getSuccessfulReadCount() {
        return successfulReads.longValue();
    }

    /**
     * Return the total number of write operations that succeeded.
     *
     * @return number of operations
     */
    public long getSuccessfulWriteCount() {
        return successfulWrites.longValue();
    }

    /**
     * Return the total number of read operations that failed.
     *
     * @return number of operations
     */
    public long getFailedReadCount() {
        return failedReads.longValue();
    }

    /**
     * Return the total number of write operations that failed.
     *
     * @return number of operations
     */
    public long getFailedWriteCount() {
        return failedWrites.longValue();
    }

    /**
     * Return the total number of read operations that were cancelled.
     *
     * @return number of operations
     */
    public long getCancelledReadCount() {
        return cancelledReads.longValue();
    }

    /**
     * Return the total number of write operations that were cancelled.
     *
     * @return number of operations
     */
    public long getCancelledWriteCount() {
        return cancelledWrites.longValue();
    }

    /**
     * Returns the current number of active read operations.
     *
     * @return number of operations
     */
    public long getCurrentReadCount() {
        return currentReads.longValue();
    }

    /**
     * Returns the current number of active write operations.
     *
     * @return number of operations
     */
    public long getCurrentWriteCount() {
        return currentWrites.longValue();
    }

    /**
     * Returns the current number of registered sockets.
     *
     * @return number of sockets
     */
    public long getCurrentRegistrationCount() {
        return currentRegistrations.longValue();
    }

}
