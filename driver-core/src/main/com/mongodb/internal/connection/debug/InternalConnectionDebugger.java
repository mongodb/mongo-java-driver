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
package com.mongodb.internal.connection.debug;

import com.mongodb.MongoInternalException;
import com.mongodb.ServerAddress;
import com.mongodb.annotations.Immutable;
import com.mongodb.annotations.NotThreadSafe;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.connection.AsyncCompletionHandler;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.Stream;
import com.mongodb.connection.StreamFactory;
import com.mongodb.internal.connection.InternalConnection;
import com.mongodb.internal.connection.InternalStreamConnection.CommandMessageData;
import com.mongodb.internal.connection.ReplyHeader;
import com.mongodb.lang.Nullable;
import org.bson.ByteBuf;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.StampedLock;
import java.util.stream.Collectors;

import static com.mongodb.assertions.Assertions.assertFalse;
import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.internal.connection.debug.Debugger.LOGGER;
import static com.mongodb.internal.connection.debug.Debugger.RUN_ID;
import static com.mongodb.internal.connection.debug.Debugger.reportingMode;
import static com.mongodb.internal.connection.debug.VisibleForTesting.AccessModifier.PRIVATE;

/**
 * A debugger for investigating issues with implementations of {@link InternalConnection} and their usage.
 * Each instance of {@link InternalConnection} must use its own instance of {@link InternalConnectionDebugger}.
 */
@ThreadSafe
public final class InternalConnectionDebugger implements Reporter {
    /**
     * Produces debugger IDs unique among all instances of {@link InternalConnectionDebugger}.
     */
    private static final AtomicLong DEBUGGER_ID_GENERATOR = new AtomicLong();
    /**
     * Produces message IDs unique among all instances of {@link InternalConnectionDebugger}.
     */
    private static final AtomicLong MESSAGE_ID_GENERATOR = new AtomicLong();

    private final long id;
    private final Debugger.ReportingMode mode;
    @Nullable
    private final DataCollector dataCollector;
    @Nullable
    private final CommandEventSenderDebugger commandEventSenderDebugger;

    public InternalConnectionDebugger() {
        this(reportingMode());
    }

    @VisibleForTesting(otherwise = PRIVATE)
    InternalConnectionDebugger(final Debugger.ReportingMode mode) {
        id = DEBUGGER_ID_GENERATOR.incrementAndGet();
        this.mode = mode;
        dataCollector = mode.on() ? new DataCollector(this) : null;
        commandEventSenderDebugger = dataCollector == null ? null : new CommandEventSenderDebugger(dataCollector);
    }

    public Optional<CommandEventSenderDebugger> commandEventSenderDebugger() {
        return Optional.ofNullable(commandEventSenderDebugger);
    }

    public StreamFactory debuggableStreamFactory(final StreamFactory factory) {
        return mode.on() ? new DebuggableStreamFactory(factory) : factory;
    }

    public void connectionOpened(final ConnectionDescription connectionDescription, final ServerDescription initialServerDescription) {
        if (mode.on()) {
            assertNotNull(dataCollector).connectionOpened(connectionDescription, initialServerDescription);
        }
    }

    public void validMessageHeader(final Object messageHeader, final int messageLength) {
        if (mode.on()) {
            assertNotNull(dataCollector);
            dataCollector.ioCollector().succeededMessageHeader(messageLength);
            dataCollector.internalConnectionOperationCollector()
                    .succeeded(null, InternalConnectionOperationCode.DECODE_MESSAGE_HEADER,
                            dataCollector.ioCollector().streamReadPosition(), messageHeader);
        }
    }

    /**
     * Reacts to errors like "message length 1953853033 is greater than the maximum message length 48000000".
     */
    public void invalidMessageHeader(final MongoInternalException e, final Object messageHeader) {
        if (mode.on()) {
            assertNotNull(dataCollector).internalConnectionOperationCollector()
                    .failed(new MongoDebuggingException("Detected an invalid reply message header " + messageHeader, e), null,
                            InternalConnectionOperationCode.DECODE_MESSAGE_HEADER,
                            dataCollector.ioCollector().streamReadPosition(), messageHeader);
        }
    }

    /**
     * Reacts to errors like "Unexpected reply message opCode 50361199".
     */
    public void invalidReplyHeader(final MongoInternalException e, final ReplyHeader replyHeader) {
        if (mode.on()) {
            assertNotNull(dataCollector).internalConnectionOperationCollector()
                    .failed(new MongoDebuggingException("Detected an invalid reply header " + replyHeader, e), null,
                            InternalConnectionOperationCode.DECODE_REPLY_HEADER,
                            dataCollector.ioCollector().streamReadPosition(), replyHeader);
        }
    }

    /**
     * Reacts to errors like "response does not match the requestId".
     */
    public void invalidReply(final MongoInternalException e) {
        if (mode.on()) {
            assertNotNull(dataCollector).internalConnectionOperationCollector()
                    .failed(new MongoDebuggingException("Detected an invalid reply", e), null,
                            InternalConnectionOperationCode.DECODE_REPLY,
                            dataCollector.ioCollector().streamReadPosition());
        }
    }

    @Override
    public boolean report(final MongoDebuggingException e, @Nullable final FailureCallback callback) {
        switch (mode) {
            case LOG: {
                LOGGER.error("", exception(e));
                return false;
            }
            case LOG_AND_THROW: {
                MongoDebuggingException reported = exception(e);
                LOGGER.error("", reported);
                if (callback == null) {
                    throw reported;
                } else {
                    callback.execute(reported);
                }
                return true;
            }
            case OFF: {
                return false;
            }
            default: {
                throw new AssertionError();
            }
        }
    }

    private MongoDebuggingException exception(final Throwable t) {
        return new MongoDebuggingException(message(), t);
    }

    private String message() {
        assertTrue(mode.on());
        return InternalConnectionDebugger.class.getSimpleName()
                + " runId=" + RUN_ID
                + " debuggerId=" + id
                + " msgId=" + MESSAGE_ID_GENERATOR.incrementAndGet()
                + ", t=" + Instant.now()
                + ", thread=" + Thread.currentThread().getName()
                + ", " + assertNotNull(dataCollector).toString();
    }

    @VisibleForTesting(otherwise = PRIVATE)
    static int ringBufferIdx(final long linearElementIdx, final int ringBufferSize) {
        return Math.toIntExact(linearElementIdx >= 0
                ? linearElementIdx % ringBufferSize
                : (ringBufferSize + linearElementIdx % ringBufferSize) % ringBufferSize);
    }

    private enum StreamOperationCode {
        OPEN,
        READ,
        WRITE,
        CLOSE
    }

    private enum InternalConnectionOperationCode {
        COMMAND,
        DECODE_MESSAGE_HEADER,
        DECODE_REPLY_HEADER,
        DECODE_REPLY
    }

    @ThreadSafe
    static final class DataCollector {
        @Nullable
        private volatile Instant openedInstant;
        @Nullable
        private volatile String openedThreadName;
        @Nullable
        private volatile String connectionDescription;
        @Nullable
        private volatile String initialServerDescription;
        private final IoCollector ioCollector;
        private final OperationCollector<StreamOperationCode> streamOperationCollector;
        private final OperationCollector<InternalConnectionOperationCode> internalConnectionOperationCollector;

        private DataCollector(final Reporter reporter) {
            ioCollector = new IoCollector();
            streamOperationCollector = new OperationCollector<>(reporter, 8, true, true);
            internalConnectionOperationCollector = new OperationCollector<>(reporter, 8, false, false);
        }

        void connectionOpened(final ConnectionDescription connectionDescription, final ServerDescription initialServerDescription) {
            openedInstant = Instant.now();
            openedThreadName = Thread.currentThread().getName();
            this.connectionDescription = connectionDescription.toString();
            this.initialServerDescription = initialServerDescription.toString();
        }

        OperationCollector<StreamOperationCode> streamOperationCollector() {
            return streamOperationCollector;
        }

        OperationCollector<InternalConnectionOperationCode> internalConnectionOperationCollector() {
            return internalConnectionOperationCollector;
        }

        IoCollector ioCollector() {
            return ioCollector;
        }

        @Override
        public String toString() {
            return "Data{"
                    + "openedInstant=" + openedInstant
                    + ", openedThreadName=" + openedThreadName
                    + ", connectionDescription=" + connectionDescription
                    + ", initialServerDescription=" + initialServerDescription
                    + ", io=" + ioCollector
                    + ", streamOperations=" + streamOperationCollector
                    + ", internalConnectionOperations=" + internalConnectionOperationCollector
                    + '}';
        }

        @NotThreadSafe
        private static final class IoData {
            private long successReads;
            private long successReadBytes;
            private long msgHeaders;
            /**
             * The sum of {@code MessageHeader.messageLength} for all successfully decoded message headers.
             */
            private long msgHeaderMessageLengthBytes;
            private long failReads;
            private long failReadBytes;
            private long failNegativeReads;
            private long successWrites;
            private long failWrites;

            IoData() {
            }

            IoData(final IoData o) {
                successReads = o.successReads;
                successReadBytes = o.successReadBytes;
                msgHeaders = o.msgHeaders;
                msgHeaderMessageLengthBytes = o.msgHeaderMessageLengthBytes;
                failReads = o.failReads;
                failReadBytes = o.failReadBytes;
                failNegativeReads = o.failNegativeReads;
                successWrites = o.successWrites;
                failWrites = o.failWrites;
            }

            @Override
            public String toString() {
                return "IoData{"
                        + "successReads=" + successReads
                        + ", successReadBytes=" + successReadBytes
                        + ", msgHeaders=" + msgHeaders
                        + ", msgHeaderMessageLengthBytes=" + msgHeaderMessageLengthBytes
                        + ", failReads=" + failReads
                        + ", failReadBytes=" + failReadBytes
                        + ", failNegativeReads=" + failNegativeReads
                        + ", successWrites=" + successWrites
                        + ", failWrites=" + failWrites
                        + '}';
            }
        }

        @Immutable
        static final class StreamReadPosition {
            private final long successReadBytes;
            private final long msgHeaderMessageLengthBytes;

            StreamReadPosition(final long successReadBytes, final long msgHeaderMessageLengthBytes) {
                this.successReadBytes = successReadBytes;
                this.msgHeaderMessageLengthBytes = msgHeaderMessageLengthBytes;
            }

            @Override
            public String toString() {
                return "StreamReadPosition{"
                        + "successReadBytes=" + successReadBytes
                        + ", msgHeaderMessageLengthBytes=" + msgHeaderMessageLengthBytes
                        + '}';
            }
        }

        @ThreadSafe
        static final class IoCollector {
            private final Lock lock;
            private final IoData data;

            IoCollector() {
                this.lock = new StampedLock().asWriteLock();
                data = new IoData();
            }

            IoData data() {
                lock.lock();
                try {
                    return new IoData(data);
                } finally {
                    lock.unlock();
                }
            }

            StreamReadPosition streamReadPosition() {
                lock.lock();
                try {
                    return new StreamReadPosition(data.successReadBytes, data.msgHeaderMessageLengthBytes);
                } finally {
                    lock.unlock();
                }
            }

            void succeededRead(final int numberOfBytes) {
                lock.lock();
                try {
                    data.successReads++;
                    data.successReadBytes += numberOfBytes;
                } finally {
                    lock.unlock();
                }
            }

            /**
             * @return  A {@link MongoDebuggingException} to {@link #report(MongoDebuggingException, FailureCallback)}.
             */
            @Nullable
            MongoDebuggingException failedRead(final int numberOfBytes) {
                boolean negativeRead = false;
                lock.lock();
                try {
                    if (numberOfBytes >= 0) {
                        data.failReads++;
                        data.failReadBytes += numberOfBytes;
                    } else {
                        data.failNegativeReads++;
                        negativeRead = true;
                    }
                } finally {
                    lock.unlock();
                }
                if (negativeRead) {
                    /* Reacts to errors like "IllegalArgumentException: minimumReadableBytes: -1484841841 (expected: >= 0)"
                     * and "IllegalArgumentException: capacity < 0: (-16 < 0)". */
                    return new MongoDebuggingException("Detected an attempt to read negative number of bytes, "
                            + "numberOfBytes=" + numberOfBytes);
                } else {
                    return null;
                }
            }

            void succeededWrite() {
                lock.lock();
                try {
                    data.successWrites++;
                } finally {
                    lock.unlock();
                }
            }

            void failedWrite() {
                lock.lock();
                try {
                    data.failWrites++;
                } finally {
                    lock.unlock();
                }
            }

            void succeededMessageHeader(final int messageLengthBytes) {
                lock.lock();
                try {
                    data.msgHeaders++;
                    data.msgHeaderMessageLengthBytes += messageLengthBytes;
                } finally {
                    lock.unlock();
                }
            }

            @Override
            public String toString() {
                lock.lock();
                try {
                    return data.toString();
                } finally {
                    lock.unlock();
                }
            }
        }

        @NotThreadSafe
        private static final class OperationEvent<C> {
            @Nullable
            private Mode mode;
            @Nullable
            private C code;
            @Nullable
            private Type type;
            private final List<Object> attachments;

            OperationEvent() {
                attachments = new ArrayList<>();
            }

            OperationEvent<C> clearAttachments() {
                this.attachments.clear();
                return this;
            }

            /**
             * @param attachments Ignored if {@code null}; {@code null} elements in the array are also ignored.
             */
            OperationEvent<C> addAttachments(@Nullable final Object... attachments) {
                if (attachments == null) {
                    return this;
                }
                for (Object attachment : attachments) {
                    if (attachment != null) {
                        this.attachments.add(attachment);
                    }
                }
                return this;
            }

            boolean canBeFollowedBy(final OperationEvent<C> event) {
                if (code == StreamOperationCode.CLOSE) {
                    // `CLOSE` can be followed by any event because `CLOSE` is allowed to run concurrently with other operations
                    return true;
                } else if (event.code == StreamOperationCode.CLOSE) {
                    // any event can be followed by `CLOSE` because `CLOSE` is allowed to run concurrently with other operations
                    return true;
                } else if (type == null) {
                    // `this` (the current event) is not initialized, i.e., `event` must be the very first event
                    return event.type == Type.BEGIN;
                } else {
                    switch (type) {
                        case BEGIN: {
                            // `BEGIN` can be followed only by `END_...` with the same `code`
                            return (event.type == Type.END_SUCCESS || event.type == Type.END_FAILURE) && code == event.code;
                        }
                        case END_SUCCESS: {
                            // `END_SUCCESS` can be followed only by `BEGIN`
                            return event.type == Type.BEGIN;
                        }
                        case END_FAILURE: {
                            // `END_FAILURE` can be followed only by `CLOSE`, which was handled above
                            //noinspection ConstantConditions
                            return assertFalse(event.code == StreamOperationCode.CLOSE);
                        }
                        default: {
                            throw new AssertionError();
                        }
                    }
                }
            }

            private String attachmentsToString() {
                return '[' + attachments.stream().map(attachment -> {
                    String strAttachment;
                    if (attachment instanceof Throwable) {
                        strAttachment = Debugger.toString((Throwable) attachment);
                    } else {
                        strAttachment = String.valueOf(attachment);
                    }
                    return "a{" + strAttachment + '}';
                }).collect(Collectors.joining(", ")) + ']';
            }

            @Override
            public String toString() {
                return "OperationEvent{"
                        + "mode=" + mode
                        + ", code=" + code
                        + ", type=" + type
                        + ", attachments=" + attachmentsToString()
                        + '}';
            }

            enum Mode {
                SYNC("s"),
                ASYNC("a"),
                UNKNOWN("u");

                private final String value;

                Mode(final String value) {
                    this.value = value;
                }

                @Override
                public String toString() {
                    return value;
                }
            }

            enum Type {
                BEGIN("B"),
                END_SUCCESS("S"),
                END_FAILURE("F");

                private final String value;

                Type(final String value) {
                    this.value = value;
                }

                @Override
                public String toString() {
                    return value;
                }
            }
        }

        @ThreadSafe
        static final class OperationCollector<C> {
            private final Lock lock;
            private final Reporter reporter;
            private long lastEventIdx;
            private final ArrayList<OperationEvent<C>> eventHistoryRingBuffer;
            private final boolean autodetectOperationMode;
            private final boolean checkEventOrder;

            OperationCollector(final Reporter reporter, final int eventsInHistory,
                    final boolean autodetectOperationMode, final boolean checkEventOrder) {
                this.lock = new StampedLock().asWriteLock();
                this.reporter = reporter;
                lastEventIdx = -1;
                /* Since we reuse event objects, we need at least 2 events in the history to be able to access
                 * both the new and the last event. */
                assertTrue(eventsInHistory >= 2);
                eventHistoryRingBuffer = new ArrayList<>(eventsInHistory);
                for (int i = 0; i < eventsInHistory; i++) {
                    eventHistoryRingBuffer.add(new OperationEvent<>());
                }
                this.autodetectOperationMode = autodetectOperationMode;
                this.checkEventOrder = checkEventOrder;
            }

            /**
             * @param attachments See {@link OperationEvent#addAttachments(Object...)}.
             * @return See {@link Reporter#report(MongoDebuggingException, FailureCallback)}.
             */
            boolean started(@Nullable final FailureCallback callback, final C code, final Object... attachments) {
                MongoDebuggingException debuggingException = registerEvent(
                        callback == null ? OperationEvent.Mode.SYNC : OperationEvent.Mode.ASYNC,
                        code, OperationEvent.Type.BEGIN, null, attachments);
                if (debuggingException != null) {
                    return reporter.report(debuggingException, callback);
                } else {
                    return false;
                }
            }

            /**
             * @param attachments See {@link OperationEvent#addAttachments(Object...)}.
             * @return See {@link Reporter#report(MongoDebuggingException, FailureCallback)}.
             */
            boolean succeeded(@Nullable final FailureCallback callback, final C code, final Object... attachments) {
                MongoDebuggingException debuggingException = registerEvent(
                        callback == null ? OperationEvent.Mode.SYNC : OperationEvent.Mode.ASYNC,
                        code, OperationEvent.Type.END_SUCCESS, null, attachments);
                if (debuggingException != null) {
                    return reporter.report(debuggingException, callback);
                } else {
                    return false;
                }
            }

            /**
             * @param attachments See {@link OperationEvent#addAttachments(Object...)}.
             * @return See {@link Reporter#report(MongoDebuggingException, FailureCallback)}.
             */
            boolean failed(final Throwable t, @Nullable final FailureCallback callback, final C code, final Object... attachments) {
                MongoDebuggingException debuggingException = registerEvent(
                        callback == null ? OperationEvent.Mode.SYNC : OperationEvent.Mode.ASYNC,
                        code, OperationEvent.Type.END_FAILURE, t, attachments);
                if (debuggingException != null) {
                    return reporter.report(debuggingException, callback);
                } else if (t instanceof MongoDebuggingException) {
                    return reporter.report((MongoDebuggingException) t, callback);
                } else {
                    return false;
                }
            }

            /**
             * @return A {@link MongoDebuggingException} to {@link #report(MongoDebuggingException, FailureCallback)}.
             */
            @Nullable
            private MongoDebuggingException registerEvent(final OperationEvent.Mode mode, final C code, final OperationEvent.Type type,
                    @Nullable final Throwable t, final Object... attachments) {
                assertTrue(t == null || type == OperationEvent.Type.END_FAILURE);
                long newEventIdx;
                boolean invalidEventOrder;
                lock.lock();
                try {
                    long prevEventIdx = lastEventIdx;
                    newEventIdx = ++lastEventIdx;
                    OperationEvent<C> prevEvent = event(prevEventIdx);
                    OperationEvent<C> newEvent = event(newEventIdx);
                    newEvent.mode = autodetectOperationMode ? mode : OperationEvent.Mode.UNKNOWN;
                    newEvent.code = code;
                    newEvent.type = type;
                    newEvent.clearAttachments()
                            .addAttachments(attachments)
                            .addAttachments(t);
                    invalidEventOrder = checkEventOrder && !prevEvent.canBeFollowedBy(newEvent);
                } finally {
                    lock.unlock();
                }
                MongoDebuggingException result = null;
                if (invalidEventOrder) {
                    result = new MongoDebuggingException("Detected either concurrent usage or usage after error"
                            + ", eventIdx=" + newEventIdx
                            + ", mode=" + mode
                            + ", code=" + code
                            + ", type=" + type);
                }
                if (result != null && t != null) {
                    result.addSuppressed(t);
                }
                return result;
            }

            private OperationEvent<C> event(final long eventIdx) {
                return eventHistoryRingBuffer.get(ringBufferIdx(eventIdx, eventHistoryRingBuffer.size()));
            }

            private String eventHistoryToString() {
                long oldestEventIdx = Math.max(0, lastEventIdx - eventHistoryRingBuffer.size() + 1);
                StringBuilder eventHistory = new StringBuilder().append('[');
                for (int i = 0; i < eventHistoryRingBuffer.size(); i++) {
                    long eventIdx = oldestEventIdx + i;
                    eventHistory.append(eventIdx).append(':').append(event(oldestEventIdx + i));
                    if (i != eventHistoryRingBuffer.size() - 1) {
                        eventHistory.append(", ");
                    }
                }
                eventHistory.append(']');
                return eventHistory.toString();
            }

            @Override
            public String toString() {
                lock.lock();
                try {
                    return "OperationData{"
                            + "lastEventIdx=" + lastEventIdx
                            + ", history=" + eventHistoryToString()
                            + '}';
                } finally {
                    lock.unlock();
                }
            }
        }
    }

    private final class DebuggableStreamFactory implements StreamFactory {
        private final StreamFactory wrapped;

        private DebuggableStreamFactory(final StreamFactory factory) {
            wrapped = factory;
        }

        @Override
        public Stream create(final ServerAddress serverAddress) {
            return new DebuggableStream(wrapped.create(serverAddress), dataCollector);
        }
    }

    private static final class DebuggableStream implements Stream {
        private final Stream wrapped;
        private final DataCollector dataCollector;

        private DebuggableStream(final Stream stream, final DataCollector dataCollector) {
            wrapped = stream;
            this.dataCollector = assertNotNull(dataCollector);
        }

        @Override
        public ByteBuf getBuffer(final int size) {
            return wrapped.getBuffer(size);
        }

        @Override
        public void open() throws IOException {
            startedOpen(null);
            try {
                wrapped.open();
                succeededOpen(null);
            } catch (Throwable t) {
                failedOpen(t, null);
                throw t;
            }
        }

        @Override
        public void openAsync(final AsyncCompletionHandler<Void> handler) {
            if (startedOpen(handler::failed)) {
                return;
            }
            wrapped.openAsync(new AsyncCompletionHandler<Void>() {
                @Override
                public void completed(final Void result) {
                    if (succeededOpen(handler::failed)) {
                        return;
                    }
                    handler.completed(result);
                }

                @Override
                public void failed(final Throwable t) {
                    if (failedOpen(t, handler::failed)) {
                        return;
                    }
                    handler.failed(t);
                }
            });
        }

        @Override
        public void write(final List<ByteBuf> buffers) throws IOException {
            startedWrite(null);
            try {
                wrapped.write(buffers);
                succeededWrite(null);
            } catch (Throwable t) {
                failedWrite(t, null);
                throw t;
            }
        }

        @Override
        public ByteBuf read(final int numBytes) throws IOException {
            startedRead(null);
            try {
                ByteBuf result = wrapped.read(numBytes);
                succeededRead(numBytes, null);
                return result;
            } catch (Throwable t) {
                failedRead(t, null, numBytes);
                throw t;
            }
        }

        @Override
        public boolean supportsAdditionalTimeout() {
            return wrapped.supportsAdditionalTimeout();
        }

        @Override
        public ByteBuf read(final int numBytes, final int additionalTimeout) throws IOException {
            startedRead(null);
            try {
                ByteBuf result = wrapped.read(numBytes, additionalTimeout);
                dataCollector.ioCollector().succeededRead(numBytes);
                succeededRead(numBytes, null);
                return result;
            } catch (Throwable t) {
                failedRead(t, null, numBytes);
                throw t;
            }
        }

        @Override
        public void writeAsync(final List<ByteBuf> buffers, final AsyncCompletionHandler<Void> handler) {
            if (startedWrite(handler::failed)) {
                return;
            }
            wrapped.writeAsync(buffers, new AsyncCompletionHandler<Void>() {
                @Override
                public void completed(final Void result) {
                    if (succeededWrite(handler::failed)) {
                        return;
                    }
                    handler.completed(result);
                }

                @Override
                public void failed(final Throwable t) {
                    if (failedWrite(t, handler::failed)) {
                        return;
                    }
                    handler.failed(t);
                }
            });
        }

        @Override
        public void readAsync(final int numBytes, final AsyncCompletionHandler<ByteBuf> handler) {
            if (startedRead(handler::failed)) {
                return;
            }
            wrapped.readAsync(numBytes, new AsyncCompletionHandler<ByteBuf>() {
                @Override
                public void completed(final ByteBuf byteBuf) {
                    if (succeededRead(numBytes, handler::failed)) {
                        return;
                    }
                    handler.completed(byteBuf);
                }

                @Override
                public void failed(final Throwable t) {
                    if (failedRead(t, handler::failed, numBytes)) {
                        return;
                    }
                    handler.failed(t);
                }
            });
        }

        /**
         * @return See {@link Reporter#report(MongoDebuggingException, FailureCallback)}.
         */
        private boolean startedOpen(@Nullable final FailureCallback callback) {
            return dataCollector.streamOperationCollector().started(callback,
                    StreamOperationCode.OPEN, dataCollector.ioCollector().data());
        }

        /**
         * @return See {@link Reporter#report(MongoDebuggingException, FailureCallback)}.
         */
        private boolean succeededOpen(@Nullable final FailureCallback callback) {
            return dataCollector.streamOperationCollector().succeeded(callback,
                    StreamOperationCode.OPEN, dataCollector.ioCollector().data());
        }

        /**
         * @return See {@link Reporter#report(MongoDebuggingException, FailureCallback)}.
         */
        private boolean failedOpen(final Throwable t, @Nullable final FailureCallback callback) {
            return dataCollector.streamOperationCollector().failed(t, callback,
                    StreamOperationCode.OPEN, dataCollector.ioCollector().data());
        }

        /**
         * @return See {@link Reporter#report(MongoDebuggingException, FailureCallback)}.
         */
        private boolean startedRead(@Nullable final FailureCallback callback) {
            return dataCollector.streamOperationCollector().started(callback,
                    StreamOperationCode.READ, dataCollector.ioCollector().data());
        }

        /**
         * @return See {@link Reporter#report(MongoDebuggingException, FailureCallback)}.
         */
        private boolean succeededRead(final int numBytes, @Nullable final FailureCallback callback) {
            dataCollector.ioCollector().succeededRead(numBytes);
            return dataCollector.streamOperationCollector().succeeded(callback,
                    StreamOperationCode.READ, dataCollector.ioCollector().data());
        }

        /**
         * @return See {@link Reporter#report(MongoDebuggingException, FailureCallback)}.
         */
        private boolean failedRead(final Throwable t, @Nullable final FailureCallback callback, final int numBytes) {
            Throwable problem = t;
            MongoDebuggingException debuggingException = dataCollector.ioCollector().failedRead(numBytes);
            if (debuggingException != null) {
                debuggingException.addSuppressed(problem);
                problem = debuggingException;
            }
            return dataCollector.streamOperationCollector().failed(problem, callback,
                    StreamOperationCode.READ, dataCollector.ioCollector().data());
        }

        /**
         * @return See {@link Reporter#report(MongoDebuggingException, FailureCallback)}.
         */
        private boolean startedWrite(@Nullable final FailureCallback callback) {
            return dataCollector.streamOperationCollector().started(callback,
                    StreamOperationCode.WRITE, dataCollector.ioCollector().data());
        }

        /**
         * @return See {@link Reporter#report(MongoDebuggingException, FailureCallback)}.
         */
        private boolean succeededWrite(@Nullable final FailureCallback callback) {
            dataCollector.ioCollector().succeededWrite();
            return dataCollector.streamOperationCollector().succeeded(callback,
                    StreamOperationCode.WRITE, dataCollector.ioCollector().data());
        }

        /**
         * @return See {@link Reporter#report(MongoDebuggingException, FailureCallback)}.
         */
        private boolean failedWrite(final Throwable t, @Nullable final FailureCallback callback) {
            dataCollector.ioCollector().failedWrite();
            return dataCollector.streamOperationCollector().failed(t, callback,
                    StreamOperationCode.WRITE, dataCollector.ioCollector().data());
        }

        @Override
        public ServerAddress getAddress() {
            return wrapped.getAddress();
        }

        @Override
        public void close() {
            dataCollector.streamOperationCollector().started(null,
                    StreamOperationCode.CLOSE, dataCollector.ioCollector().data());
            try {
                wrapped.close();
                dataCollector.streamOperationCollector().succeeded(null,
                        StreamOperationCode.CLOSE, dataCollector.ioCollector().data());
            } catch (Throwable t) {
                dataCollector.streamOperationCollector().failed(t, null,
                        StreamOperationCode.CLOSE, dataCollector.ioCollector().data());
                throw t;
            }
        }

        @Override
        public boolean isClosed() {
            return wrapped.isClosed();
        }
    }

    @ThreadSafe
    public static final class CommandEventSenderDebugger {
        private final DataCollector dataCollector;

        CommandEventSenderDebugger(final DataCollector dataCollector) {
            this.dataCollector = assertNotNull(dataCollector);
        }

        public void sendStartedEvent(final CommandMessageData commandMessageData) {
            dataCollector.internalConnectionOperationCollector().started(null, InternalConnectionOperationCode.COMMAND,
                    dataCollector.ioCollector().streamReadPosition(), commandMessageData);
        }

        public void sendFailedEvent(final Throwable t, final CommandMessageData commandMessageData) {
            dataCollector.internalConnectionOperationCollector().failed(t, null, InternalConnectionOperationCode.COMMAND,
                    dataCollector.ioCollector().streamReadPosition(), commandMessageData);
        }

        public void sendSucceededEvent(final CommandMessageData commandMessageData) {
            sendSucceededEventForOneWayCommand(commandMessageData);
        }

        public void sendSucceededEventForOneWayCommand(final CommandMessageData commandMessageData) {
            dataCollector.internalConnectionOperationCollector().succeeded(null, InternalConnectionOperationCode.COMMAND,
                    dataCollector.ioCollector().streamReadPosition(), commandMessageData);
        }
    }
}
