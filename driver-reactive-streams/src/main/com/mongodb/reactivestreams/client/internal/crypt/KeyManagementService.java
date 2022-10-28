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
import com.mongodb.ServerAddress;
import com.mongodb.connection.AsyncCompletionHandler;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.connection.Stream;
import com.mongodb.connection.StreamFactory;
import com.mongodb.connection.TlsChannelStreamFactoryFactory;
import com.mongodb.crypt.capi.MongoKeyDecryptor;
import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.internal.diagnostics.logging.Loggers;
import com.mongodb.internal.connection.AsynchronousChannelStream;
import org.bson.ByteBuf;
import org.bson.ByteBufNIO;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import javax.net.ssl.SSLContext;
import java.io.Closeable;
import java.nio.channels.CompletionHandler;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class KeyManagementService implements Closeable {
    private static final Logger LOGGER = Loggers.getLogger("client");
    private final Map<String, SSLContext> kmsProviderSslContextMap;
    private final int timeoutMillis;
    private final TlsChannelStreamFactoryFactory tlsChannelStreamFactoryFactory;

    KeyManagementService(final Map<String, SSLContext> kmsProviderSslContextMap, final int timeoutMillis) {
        this.kmsProviderSslContextMap = kmsProviderSslContextMap;
        this.tlsChannelStreamFactoryFactory = new TlsChannelStreamFactoryFactory();
        this.timeoutMillis = timeoutMillis;
    }

    public void close() {
        tlsChannelStreamFactoryFactory.close();
    }

    Mono<Void> decryptKey(final MongoKeyDecryptor keyDecryptor) {
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
            stream.openAsync(new AsyncCompletionHandler<Void>() {
                @Override
                public void completed(final Void ignored) {
                    streamWrite(stream, keyDecryptor, sink);
                }

                @Override
                public void failed(final Throwable t) {
                    stream.close();
                    sink.error(t);
                }
            });
        }).onErrorMap(this::unWrapException);
    }

    private void streamWrite(final Stream stream, final MongoKeyDecryptor keyDecryptor, final MonoSink<Void> sink) {
        List<ByteBuf> byteBufs = singletonList(new ByteBufNIO(keyDecryptor.getMessage()));
        stream.writeAsync(byteBufs, new AsyncCompletionHandler<Void>() {
            @Override
            public void completed(final Void aVoid) {
                streamRead(stream, keyDecryptor, sink);
            }

            @Override
            public void failed(final Throwable t) {
                stream.close();
                sink.error(t);
            }
        });
    }

    private void streamRead(final Stream stream, final MongoKeyDecryptor keyDecryptor, final MonoSink<Void> sink) {
        int bytesNeeded = keyDecryptor.bytesNeeded();
        if (bytesNeeded > 0) {
            AsynchronousChannelStream asyncStream = (AsynchronousChannelStream) stream;
            final ByteBuf buffer = asyncStream.getBuffer(bytesNeeded);
            asyncStream.getChannel().read(buffer.asNIO(), asyncStream.getSettings().getReadTimeout(MILLISECONDS), MILLISECONDS, null,
                                          new CompletionHandler<Integer, Void>() {

                                              @Override
                                              public void completed(final Integer integer, final Void aVoid) {
                                                  buffer.flip();
                                                  try {
                                                      keyDecryptor.feed(buffer.asNIO());
                                                      buffer.release();
                                                      streamRead(stream, keyDecryptor, sink);
                                                  } catch (Throwable t) {
                                                      sink.error(t);
                                                  }
                                              }

                                              @Override
                                              public void failed(final Throwable t, final Void aVoid) {
                                                  buffer.release();
                                                  stream.close();
                                                  sink.error(t);
                                              }
                                          });
        } else {
            stream.close();
            sink.success();
        }
    }

    private Throwable unWrapException(final Throwable t) {
        return t instanceof MongoSocketException ? t.getCause() : t;
    }
}
