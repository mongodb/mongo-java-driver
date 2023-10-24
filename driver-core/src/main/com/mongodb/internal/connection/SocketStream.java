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

package com.mongodb.internal.connection;

import com.mongodb.MongoSocketException;
import com.mongodb.MongoSocketOpenException;
import com.mongodb.MongoSocketReadException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.AsyncCompletionHandler;
import com.mongodb.connection.BufferProvider;
import com.mongodb.connection.ProxySettings;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.connection.Stream;
import com.mongodb.internal.async.SingleResultCallback;
import org.bson.ByteBuf;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.connection.SocketStreamHelper.configureSocket;
import static com.mongodb.internal.connection.SslHelper.configureSslSocket;
import static com.mongodb.internal.thread.InterruptionUtil.translateInterruptedException;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
@SuppressWarnings("deprecation")
public class SocketStream implements Stream {
    private final ServerAddress address;
    private final SocketSettings settings;
    private final SslSettings sslSettings;
    private final SocketFactory socketFactory;
    private final BufferProvider bufferProvider;
    private volatile Socket socket;
    private volatile OutputStream outputStream;
    private volatile InputStream inputStream;
    private volatile boolean isClosed;

    public SocketStream(final ServerAddress address, final SocketSettings settings, final SslSettings sslSettings,
                        final SocketFactory socketFactory, final BufferProvider bufferProvider) {
        this.address = notNull("address", address);
        this.settings = notNull("settings", settings);
        this.sslSettings = notNull("sslSettings", sslSettings);
        this.socketFactory = notNull("socketFactory", socketFactory);
        this.bufferProvider = notNull("bufferProvider", bufferProvider);
    }

    @Override
    public void open() {
        try {
            socket = initializeSocket();
            outputStream = socket.getOutputStream();
            inputStream = socket.getInputStream();
        } catch (IOException e) {
            close();
            throw translateInterruptedException(e, "Interrupted while connecting")
                    .orElseThrow(() -> new MongoSocketOpenException("Exception opening socket", getAddress(), e));
        }
    }

    @SuppressWarnings("deprecation")
    protected Socket initializeSocket() throws IOException {
        ProxySettings proxySettings = settings.getProxySettings();
        if (proxySettings.isProxyEnabled()) {
            if (sslSettings.isEnabled()) {
                assertTrue(socketFactory instanceof SSLSocketFactory);
                SSLSocketFactory sslSocketFactory = (SSLSocketFactory) socketFactory;
                return initializeSslSocketOverSocksProxy(sslSocketFactory);
            }
            return initializeSocketOverSocksProxy();
        }

        Iterator<InetSocketAddress> inetSocketAddresses = address.getSocketAddresses().iterator();
        while (inetSocketAddresses.hasNext()) {
            Socket socket = socketFactory.createSocket();
            try {
                SocketStreamHelper.initialize(socket, inetSocketAddresses.next(), settings, sslSettings);
                return socket;
            } catch (SocketTimeoutException e) {
                if (!inetSocketAddresses.hasNext()) {
                    throw e;
                }
            }
        }

        throw new MongoSocketException("Exception opening socket", getAddress());
    }

    private SSLSocket initializeSslSocketOverSocksProxy(final SSLSocketFactory sslSocketFactory) throws IOException {
        final String serverHost = address.getHost();
        final int serverPort = address.getPort();

        SocksSocket socksProxy = new SocksSocket(settings.getProxySettings());
        configureSocket(socksProxy, settings);
        InetSocketAddress inetSocketAddress = toSocketAddress(serverHost, serverPort);
        socksProxy.connect(inetSocketAddress, settings.getConnectTimeout(MILLISECONDS));

        SSLSocket sslSocket = (SSLSocket) sslSocketFactory.createSocket(socksProxy, serverHost, serverPort, true);
        //Even though Socks proxy connection is already established, TLS handshake has not been performed yet.
        //So it is possible to set SSL parameters before handshake is done.
        configureSslSocket(sslSocket, sslSettings, inetSocketAddress);
        return sslSocket;
    }


    /**
     * Creates an unresolved {@link InetSocketAddress}.
     * This method is used to create an address that is meant to be resolved by a SOCKS proxy.
     */
    private static InetSocketAddress toSocketAddress(final String serverHost, final int serverPort) {
        return InetSocketAddress.createUnresolved(serverHost, serverPort);
    }

    private Socket initializeSocketOverSocksProxy() throws IOException {
        Socket createdSocket = socketFactory.createSocket();
        configureSocket(createdSocket, settings);
        /*
          Wrap the configured socket with SocksSocket to add extra functionality.
          Reason for separate steps: We can't directly extend Java 11 methods within 'SocksSocket'
          to configure itself.
         */
        SocksSocket socksProxy = new SocksSocket(createdSocket, settings.getProxySettings());

        socksProxy.connect(toSocketAddress(address.getHost(), address.getPort()),
                settings.getConnectTimeout(TimeUnit.MILLISECONDS));
        return socksProxy;
    }

    @Override
    public ByteBuf getBuffer(final int size) {
        return bufferProvider.getBuffer(size);
    }

    @Override
    public void write(final List<ByteBuf> buffers) throws IOException {
        for (final ByteBuf cur : buffers) {
            outputStream.write(cur.array(), 0, cur.limit());
        }
    }

    @Override
    public ByteBuf read(final int numBytes) throws IOException {
        ByteBuf buffer = bufferProvider.getBuffer(numBytes);
        try {
            int totalBytesRead = 0;
            byte[] bytes = buffer.array();
            while (totalBytesRead < buffer.limit()) {
                int bytesRead = inputStream.read(bytes, totalBytesRead, buffer.limit() - totalBytesRead);
                if (bytesRead == -1) {
                    throw new MongoSocketReadException("Prematurely reached end of stream", getAddress());
                }
                totalBytesRead += bytesRead;
            }
            return buffer;
        } catch (Exception e) {
            buffer.release();
            throw e;
        }
    }

    @Override
    public boolean supportsAdditionalTimeout() {
        return true;
    }

    @Override
    public ByteBuf read(final int numBytes, final int additionalTimeout) throws IOException {
        int curTimeout = socket.getSoTimeout();
        if (curTimeout > 0 && additionalTimeout > 0) {
            socket.setSoTimeout(curTimeout + additionalTimeout);
        }
        try {
            return read(numBytes);
        } finally {
            if (!socket.isClosed()) {
                // `socket` may be closed if the current thread is virtual, and it is interrupted while reading
                socket.setSoTimeout(curTimeout);
            }
        }
    }

    @Override
    public void openAsync(final SingleResultCallback<Void> callback) {
        throw new UnsupportedOperationException(getClass() + " does not support asynchronous operations.");
    }

    @Override
    public void writeAsync(final List<ByteBuf> buffers, final AsyncCompletionHandler<Void> handler) {
        throw new UnsupportedOperationException(getClass() + " does not support asynchronous operations.");
    }

    @Override
    public void readAsync(final int numBytes, final AsyncCompletionHandler<ByteBuf> handler) {
        throw new UnsupportedOperationException(getClass() + " does not support asynchronous operations.");
    }

    @Override
    public ServerAddress getAddress() {
        return address;
    }

    /**
     * Get the settings for this socket.
     *
     * @return the settings
     */
    SocketSettings getSettings() {
        return settings;
    }

    @Override
    public void close() {
        try {
            isClosed = true;
            if (socket != null) {
                socket.close();
            }
        } catch (IOException | RuntimeException e) {
            // ignore
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }
}
