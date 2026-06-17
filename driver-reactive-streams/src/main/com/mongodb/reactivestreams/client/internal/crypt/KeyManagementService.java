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

package com.mongodb.reactivestreams.client.internal.crypt;

import com.mongodb.MongoException;
import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoSocketReadTimeoutException;
import com.mongodb.MongoSocketWriteTimeoutException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.AsyncCompletionHandler;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.VisibleForTesting;
import com.mongodb.internal.connection.AsynchronousChannelStream;
import com.mongodb.internal.connection.DefaultInetAddressResolver;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.connection.Stream;
import com.mongodb.internal.connection.StreamFactory;
import com.mongodb.internal.connection.TlsChannelStreamFactoryFactory;
import com.mongodb.internal.crypt.capi.MongoKeyDecryptor;
import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.internal.diagnostics.logging.Loggers;
import com.mongodb.internal.time.Timeout;
import com.mongodb.lang.NonNull;
import com.mongodb.lang.Nullable;
import org.bson.ByteBuf;
import org.bson.ByteBufNIO;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import javax.net.ssl.SSLContext;
import java.io.Closeable;
import java.nio.channels.CompletionHandler;
import java.nio.channels.InterruptedByTimeoutException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;
import static com.mongodb.internal.capi.MongoCryptHelper.KMS_TIMEOUT_ERROR_MESSAGE;
import static com.mongodb.internal.capi.MongoCryptHelper.checkKmsRetryBackoff;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.bson.assertions.Assertions.assertTrue;

class KeyManagementService implements Closeable {
    private static final Logger LOGGER = Loggers.getLogger("client");
    private final Map<String, SSLContext> kmsProviderSslContextMap;
    private final int timeoutMillis;
    private final TlsChannelStreamFactoryFactory tlsChannelStreamFactoryFactory;

    KeyManagementService(final Map<String, SSLContext> kmsProviderSslContextMap, final int timeoutMillis) {
        this(kmsProviderSslContextMap, timeoutMillis, new TlsChannelStreamFactoryFactory(new DefaultInetAddressResolver()));
    }

    @VisibleForTesting(otherwise = PRIVATE)
    KeyManagementService(final Map<String, SSLContext> kmsProviderSslContextMap, final int timeoutMillis,
                         final TlsChannelStreamFactoryFactory tlsChannelStreamFactoryFactory) {
        assertTrue("timeoutMillis > 0", timeoutMillis > 0);
        this.kmsProviderSslContextMap = kmsProviderSslContextMap;
        this.tlsChannelStreamFactoryFactory = tlsChannelStreamFactoryFactory;
        this.timeoutMillis = timeoutMillis;
    }

    public void close() {
        tlsChannelStreamFactoryFactory.close();
    }

    Mono<Void> decryptKey(final MongoKeyDecryptor keyDecryptor, @Nullable final Timeout operationTimeout) {
        return Mono.defer(() -> {
            long sleepMicros = keyDecryptor.sleepMicroseconds();
            if (sleepMicros > 0) {
                checkKmsRetryBackoff(operationTimeout, sleepMicros);
                return Mono.delay(Duration.of(sleepMicros, ChronoUnit.MICROS))
                        .then(attemptDecryptKey(keyDecryptor, operationTimeout));
            }
            return attemptDecryptKey(keyDecryptor, operationTimeout);
        }).onErrorMap(this::unWrapException);
    }

    private Mono<Void> attemptDecryptKey(final MongoKeyDecryptor keyDecryptor, @Nullable final Timeout operationTimeout) {
        SocketSettings socketSettings = SocketSettings.builder()
                .connectTimeout(timeoutMillis, MILLISECONDS)
                .readTimeout(timeoutMillis, MILLISECONDS)
                .build();
        StreamFactory streamFactory = tlsChannelStreamFactoryFactory.create(socketSettings,
                SslSettings.builder().enabled(true).context(kmsProviderSslContextMap.get(keyDecryptor.getKmsProvider())).build());

        ServerAddress serverAddress = new ServerAddress(keyDecryptor.getHostName());

        LOGGER.info("Connecting to KMS server at " + serverAddress);

        return Mono.<Void>create(sink -> {
            OperationContext operationContext = createOperationContext(operationTimeout, socketSettings);
            Stream stream = streamFactory.create(serverAddress);
            stream.openAsync(operationContext, new AsyncCompletionHandler<Void>() {
                @Override
                public void completed(@Nullable final Void ignored) {
                    try {
                        streamWrite(stream, keyDecryptor, operationContext, operationTimeout, sink);
                    } catch (Throwable t) {
                        stream.close();
                        sink.error(t);
                    }
                }

                @Override
                public void failed(final Throwable t) {
                    stream.close();
                    failOrHandleError(t, keyDecryptor, operationTimeout, sink);
                }
            });
        });
    }

    private void streamWrite(final Stream stream, final MongoKeyDecryptor keyDecryptor,
                             final OperationContext operationContext, @Nullable final Timeout operationTimeout,
                             final MonoSink<Void> sink) {
        List<ByteBuf> byteBufs = singletonList(new ByteBufNIO(keyDecryptor.getMessage()));
        stream.writeAsync(byteBufs, operationContext, new AsyncCompletionHandler<Void>() {
            @Override
            public void completed(@Nullable final Void aVoid) {
                try {
                    streamRead(stream, keyDecryptor, operationContext, operationTimeout, sink);
                } catch (Throwable t) {
                    stream.close();
                    sink.error(t);
                }
            }

            @Override
            public void failed(final Throwable t) {
                stream.close();
                failOrHandleError(t, keyDecryptor, operationTimeout, sink);
            }
        });
    }

    private void streamRead(final Stream stream, final MongoKeyDecryptor keyDecryptor,
                            final OperationContext operationContext, @Nullable final Timeout operationTimeout,
                            final MonoSink<Void> sink) {
        int bytesNeeded = keyDecryptor.bytesNeeded();
        if (bytesNeeded <= 0) {
            stream.close();
            sink.success();
            return;
        }
        AsynchronousChannelStream asyncStream = (AsynchronousChannelStream) stream;
        ByteBuf buffer = asyncStream.getBuffer(bytesNeeded);
        CompletionHandler<Integer, Void> readHandler = new CompletionHandler<Integer, Void>() {

            @Override
            public void completed(final Integer integer, final Void aVoid) {
                try {
                    if (integer == -1) {
                        buffer.release();
                        stream.close();
                        // Treat an unexpected end of stream (the KMS server closed the connection) as a retryable
                        // transient network error: hand it to failOrHandleError so the context is retried if budget allows.
                        MongoException eof = new MongoException("Unexpected end of stream from KMS provider "
                                + keyDecryptor.getKmsProvider());
                        failOrHandleError(eof, keyDecryptor, operationTimeout, sink);
                        return;
                    }
                    buffer.flip();
                    boolean shouldRetry;
                    try {
                        shouldRetry = keyDecryptor.feedWithRetry(buffer.asNIO());
                    } finally {
                        buffer.release();
                    }
                    if (shouldRetry) {
                        // libmongocrypt marked the context for retry; complete this attempt and let the state machine re-present it
                        stream.close();
                        sink.success();
                    } else {
                        streamRead(stream, keyDecryptor, operationContext, operationTimeout, sink);
                    }
                } catch (Throwable t) {
                    stream.close();
                    sink.error(t);
                }
            }

            @Override
            public void failed(final Throwable t, final Void aVoid) {
                buffer.release();
                stream.close();
                failOrHandleError(t, keyDecryptor, operationTimeout, sink);
            }
        };
        try {
            long readTimeoutMS = operationContext.getTimeoutContext().getReadTimeoutMS();
            asyncStream.getChannel().read(buffer.asNIO(), readTimeoutMS, MILLISECONDS, null, readHandler);
        } catch (RuntimeException | Error e) {
            // the handler was not invoked, so the buffer must be released here
            buffer.release();
            throw e;
        }
    }

    private static void failOrHandleError(final Throwable t, final MongoKeyDecryptor keyDecryptor,
            @Nullable final Timeout operationTimeout, final MonoSink<Void> sink) {
        if (isTimeoutException(t) && hasExpired(operationTimeout)) {
            sink.error(TimeoutContext.createMongoTimeoutException(KMS_TIMEOUT_ERROR_MESSAGE, t));
            return;
        }
        if (keyDecryptor.fail()) {
            LOGGER.debug("Retrying KMS request after transient error", t);
            sink.success();
        } else {
            sink.error(t);
        }
    }

    private static boolean hasExpired(@Nullable final Timeout operationTimeout) {
        return operationTimeout != null && operationTimeout.call(MILLISECONDS,
                () -> false,
                remainingMillis -> false,
                () -> true);
    }

    private OperationContext createOperationContext(@Nullable final Timeout operationTimeout, final SocketSettings socketSettings) {
        TimeoutSettings timeoutSettings;
        if (operationTimeout == null) {
            timeoutSettings = createTimeoutSettings(socketSettings, null);
        } else {
            timeoutSettings = operationTimeout.call(MILLISECONDS,
                    () -> {
                        throw new AssertionError("operationTimeout cannot be infinite");
                    },
                    (ms) -> createTimeoutSettings(socketSettings, ms),
                    () -> {
                        throw new MongoOperationTimeoutException(KMS_TIMEOUT_ERROR_MESSAGE);
                    });
        }
        return OperationContext.simpleOperationContext(new TimeoutContext(timeoutSettings));
    }

    @NonNull
    private static TimeoutSettings createTimeoutSettings(final SocketSettings socketSettings,
            @Nullable final Long ms) {
        return new TimeoutSettings(
                0,
                socketSettings.getConnectTimeout(MILLISECONDS),
                socketSettings.getReadTimeout(MILLISECONDS),
                ms,
                0);
    }

    private Throwable unWrapException(final Throwable t) {
        // openAsync wraps IOException in MongoSocketOpenException; unwrap it to match the sync path, which throws IOException directly.
        // Timeout subclasses (MongoSocketReadTimeoutException, MongoSocketWriteTimeoutException) carry meaningful type identity
        // used by isTimeoutException(), so preserve them rather than unwrapping.
        if (t instanceof MongoSocketReadTimeoutException || t instanceof MongoSocketWriteTimeoutException) {
            return t;
        }
        Throwable cause = t.getCause();
        return t instanceof MongoSocketException && cause != null ? cause : t;
    }

    private static boolean isTimeoutException(final Throwable t) {
        return t instanceof MongoSocketReadTimeoutException
                || t instanceof MongoSocketWriteTimeoutException
                || t instanceof InterruptedByTimeoutException;
    }
}
