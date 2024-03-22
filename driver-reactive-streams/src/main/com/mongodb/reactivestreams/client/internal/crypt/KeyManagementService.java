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

import com.mongodb.MongoSocketException;
import com.mongodb.MongoSocketReadTimeoutException;
import com.mongodb.MongoSocketWriteTimeoutException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.AsyncCompletionHandler;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.crypt.capi.MongoKeyDecryptor;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.connection.AsynchronousChannelStream;
import com.mongodb.internal.connection.DefaultInetAddressResolver;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.connection.Stream;
import com.mongodb.internal.connection.StreamFactory;
import com.mongodb.internal.connection.TlsChannelStreamFactoryFactory;
import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.internal.diagnostics.logging.Loggers;
import com.mongodb.internal.time.Timeout;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.internal.TimeoutHelper;
import org.bson.ByteBuf;
import org.bson.ByteBufNIO;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import javax.net.ssl.SSLContext;
import java.io.Closeable;
import java.nio.channels.CompletionHandler;
import java.nio.channels.InterruptedByTimeoutException;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class KeyManagementService implements Closeable {
    private static final Logger LOGGER = Loggers.getLogger("client");
    private static final String TIMEOUT_ERROR_MESSAGE = "KMS key decryption exceeded the timeout limit.";
    private final Map<String, SSLContext> kmsProviderSslContextMap;
    private final int timeoutMillis;
    private final TlsChannelStreamFactoryFactory tlsChannelStreamFactoryFactory;

    KeyManagementService(final Map<String, SSLContext> kmsProviderSslContextMap, final int timeoutMillis) {
        this.kmsProviderSslContextMap = kmsProviderSslContextMap;
        this.tlsChannelStreamFactoryFactory = new TlsChannelStreamFactoryFactory(new DefaultInetAddressResolver());
        this.timeoutMillis = timeoutMillis;
    }

    public void close() {
        tlsChannelStreamFactoryFactory.close();
    }

    @SuppressWarnings("deprecation") //readTimeout
    Mono<Void> decryptKey(final MongoKeyDecryptor keyDecryptor, @Nullable final Timeout operationTimeout) {
        SocketSettings socketSettings = SocketSettings.builder()
                .connectTimeout(timeoutMillis, MILLISECONDS)
                .readTimeout(timeoutMillis, MILLISECONDS)
                .build();
        StreamFactory streamFactory = tlsChannelStreamFactoryFactory.create(socketSettings,
                SslSettings.builder().enabled(true).context(kmsProviderSslContextMap.get(keyDecryptor.getKmsProvider())).build());

        ServerAddress serverAddress = new ServerAddress(keyDecryptor.getHostName());

        LOGGER.info("Connecting to KMS server at " + serverAddress);

        return Mono.<Void>create(sink -> {
            Stream stream = streamFactory.create(serverAddress);
            OperationContext operationContext = createOperationContext(operationTimeout, socketSettings);
            stream.openAsync(operationContext, new AsyncCompletionHandler<Void>() {
                @Override
                public void completed(@Nullable final Void ignored) {
                    streamWrite(stream, keyDecryptor, operationContext, sink);
                }

                @Override
                public void failed(final Throwable t) {
                    stream.close();
                    handleError(t, operationContext, sink);
                }
            });
        }).onErrorMap(this::unWrapException);
    }

    private void streamWrite(final Stream stream, final MongoKeyDecryptor keyDecryptor,
                             final OperationContext operationContext, final MonoSink<Void> sink) {
        List<ByteBuf> byteBufs = singletonList(new ByteBufNIO(keyDecryptor.getMessage()));
        stream.writeAsync(byteBufs, operationContext, new AsyncCompletionHandler<Void>() {
            @Override
            public void completed(@Nullable final Void aVoid) {
                streamRead(stream, keyDecryptor, operationContext, sink);
            }

            @Override
            public void failed(final Throwable t) {
                stream.close();
                handleError(t, operationContext, sink);
            }
        });
    }

    private void streamRead(final Stream stream, final MongoKeyDecryptor keyDecryptor,
                            final OperationContext operationContext, final MonoSink<Void> sink) {
        int bytesNeeded = keyDecryptor.bytesNeeded();
        if (bytesNeeded > 0) {
            AsynchronousChannelStream asyncStream = (AsynchronousChannelStream) stream;
            ByteBuf buffer = asyncStream.getBuffer(bytesNeeded);
            long readTimeoutMS = operationContext.getTimeoutContext().getReadTimeoutMS();
            asyncStream.getChannel().read(buffer.asNIO(), readTimeoutMS, MILLISECONDS, null,
                                          new CompletionHandler<Integer, Void>() {

                                              @Override
                                              public void completed(final Integer integer, final Void aVoid) {
                                                  buffer.flip();
                                                  try {
                                                      keyDecryptor.feed(buffer.asNIO());
                                                      buffer.release();
                                                      streamRead(stream, keyDecryptor, operationContext, sink);
                                                  } catch (Throwable t) {
                                                      sink.error(t);
                                                  }
                                              }

                                              @Override
                                              public void failed(final Throwable t, final Void aVoid) {
                                                  buffer.release();
                                                  stream.close();
                                                  handleError(t, operationContext, sink);
                                              }
                                          });
        } else {
            stream.close();
            sink.success();
        }
    }

    private static void handleError(final Throwable t, final OperationContext operationContext, final MonoSink<Void> sink) {
        if (isTimeoutException(t) && operationContext.getTimeoutContext().hasTimeoutMS()) {
            sink.error(TimeoutContext.createMongoTimeoutException(TIMEOUT_ERROR_MESSAGE, t));
        } else {
            sink.error(t);
        }
    }

    @SuppressWarnings("deprecation") //readTimeout
    private OperationContext createOperationContext(@Nullable final Timeout operationTimeout, final SocketSettings socketSettings) {
        return OperationContext.simpleOperationContext(new TimeoutContext(
                new TimeoutSettings(
                        0,
                        socketSettings.getConnectTimeout(MILLISECONDS),
                        socketSettings.getReadTimeout(MILLISECONDS),
                        getRemaining(operationTimeout),
                        0))
        );
    }

    @Nullable
    private static Long getRemaining(@Nullable final Timeout operationTimeout) {
        if (operationTimeout == null) {
            return null;
        }
        return TimeoutHelper.getRemainingMs(TIMEOUT_ERROR_MESSAGE, operationTimeout);
    }

    private Throwable unWrapException(final Throwable t) {
        return t instanceof MongoSocketException ? t.getCause() : t;
    }

    private static boolean isTimeoutException(final Throwable t) {
        return t instanceof MongoSocketReadTimeoutException
                || t instanceof MongoSocketWriteTimeoutException
                || t instanceof InterruptedByTimeoutException;
    }
}
