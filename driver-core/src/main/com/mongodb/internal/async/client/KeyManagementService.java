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

package com.mongodb.internal.async.client;

import com.mongodb.MongoSocketException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.AsyncCompletionHandler;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.connection.Stream;
import com.mongodb.connection.StreamFactory;
import com.mongodb.connection.TlsChannelStreamFactoryFactory;
import com.mongodb.crypt.capi.MongoKeyDecryptor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.connection.AsynchronousChannelStream;
import org.bson.ByteBuf;
import org.bson.ByteBufNIO;

import javax.net.ssl.SSLContext;
import java.nio.channels.CompletionHandler;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

class KeyManagementService {
    private final int defaultPort;
    private final TlsChannelStreamFactoryFactory tlsChannelStreamFactoryFactory;
    private final StreamFactory streamFactory;

    KeyManagementService(final SSLContext sslContext, final int defaultPort, final int timeoutMillis) {
        this.defaultPort = defaultPort;
        this.tlsChannelStreamFactoryFactory = new TlsChannelStreamFactoryFactory();
        this.streamFactory = tlsChannelStreamFactoryFactory.create(SocketSettings.builder()
                        .connectTimeout(timeoutMillis, TimeUnit.MILLISECONDS)
                        .readTimeout(timeoutMillis, TimeUnit.MILLISECONDS)
                        .build(),
                SslSettings.builder().enabled(true).context(sslContext).build());
    }


    public void close() {
        tlsChannelStreamFactoryFactory.close();
    }

    void decryptKey(final MongoKeyDecryptor keyDecryptor, final SingleResultCallback<Void> callback) {
        streamOpen(keyDecryptor, callback);
    }

    private void streamOpen(final MongoKeyDecryptor keyDecryptor, final SingleResultCallback<Void> callback) {
        ServerAddress serverAddress = keyDecryptor.getHostName().contains(":")
                ? new ServerAddress(keyDecryptor.getHostName())
                : new ServerAddress(keyDecryptor.getHostName(), defaultPort);
        final Stream stream = streamFactory.create(serverAddress);
        stream.openAsync(new AsyncCompletionHandler<Void>() {
            @Override
            public void completed(final Void aVoid) {
                streamWrite(stream, keyDecryptor, callback);
            }

            @Override
            public void failed(final Throwable t) {
                stream.close();
                callback.onResult(null, wrapException(t));
            }
        });
    }

    private void streamWrite(final Stream stream, final MongoKeyDecryptor keyDecryptor, final SingleResultCallback<Void> callback) {
        List<ByteBuf> byteBufs = Collections.<ByteBuf>singletonList(new ByteBufNIO(keyDecryptor.getMessage()));
        stream.writeAsync(byteBufs, new AsyncCompletionHandler<Void>() {
            @Override
            public void completed(final Void aVoid) {
                streamRead(stream, keyDecryptor, callback);
            }

            @Override
            public void failed(final Throwable t) {
                stream.close();
                callback.onResult(null, wrapException(t));
            }
        });
    }

    private void streamRead(final Stream stream, final MongoKeyDecryptor keyDecryptor, final SingleResultCallback<Void> callback) {
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
                                streamRead(stream, keyDecryptor, callback);
                            } catch (Throwable t) {
                                callback.onResult(null, t);
                            }
                        }

                        @Override
                        public void failed(final Throwable t, final Void aVoid) {
                            buffer.release();
                            stream.close();
                            callback.onResult(null, wrapException(t));
                        }
                    });
        } else {
            stream.close();
            callback.onResult(null, null);
        }
    }

    private Throwable wrapException(final Throwable t) {
        return t instanceof MongoSocketException ? t.getCause() : t;
    }
}
