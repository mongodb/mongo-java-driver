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

import com.mongodb.ClusterFixture;
import com.mongodb.MongoSocketOpenException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.TimeoutSettings;
import org.bson.ByteBuf;
import org.bson.ByteBufNIO;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.InterruptedByTimeoutException;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.getPrimaryServerDescription;
import static com.mongodb.internal.connection.OperationContext.simpleOperationContext;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TlsChannelStreamFunctionalTest {
    private static final SslSettings SSL_SETTINGS = SslSettings.builder().enabled(true).build();
    private static final String UNREACHABLE_PRIVATE_IP_ADDRESS = "10.255.255.1";
    private static final int UNREACHABLE_PORT = 65333;

    @ParameterizedTest
    @ValueSource(ints = {500, 1000, 2000})
    void shouldInterruptConnectionEstablishmentWhenConnectionTimeoutExpires(final int connectTimeoutMs) throws IOException {
        //given
        try (StreamFactoryFactory streamFactoryFactory = new TlsChannelStreamFactoryFactory(new DefaultInetAddressResolver());
             MockedStatic<SocketChannel> socketChannelMockedStatic = Mockito.mockStatic(SocketChannel.class)) {
            SingleResultSpyCaptor<SocketChannel> singleResultSpyCaptor = new SingleResultSpyCaptor<>();
            socketChannelMockedStatic.when(SocketChannel::open).thenAnswer(singleResultSpyCaptor);

            StreamFactory streamFactory = streamFactoryFactory.create(SocketSettings.builder()
                    .connectTimeout(connectTimeoutMs, TimeUnit.MILLISECONDS)
                    .build(), SSL_SETTINGS);

            Stream stream = streamFactory.create(new ServerAddress(UNREACHABLE_PRIVATE_IP_ADDRESS, UNREACHABLE_PORT));
            long connectOpenStart = System.nanoTime();

            //when
            OperationContext operationContext = createOperationContext(connectTimeoutMs);
            MongoSocketOpenException mongoSocketOpenException = assertThrows(MongoSocketOpenException.class, () ->
                    stream.open(operationContext));

            //then
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - connectOpenStart);
            // Allow for some timing imprecision due to test overhead.
            int maximumAcceptableTimeoutOvershoot = 300;

            assertInstanceOf(InterruptedByTimeoutException.class, mongoSocketOpenException.getCause());
            assertFalse(connectTimeoutMs > elapsedMs,
                    format("Connection timed-out sooner than expected. ConnectTimeoutMS: %d, elapsedMs: %d", connectTimeoutMs, elapsedMs));
            assertTrue(elapsedMs - connectTimeoutMs <= maximumAcceptableTimeoutOvershoot,
                    format("Connection timeout overshoot time %d ms should be within %d ms", elapsedMs - connectTimeoutMs,
                            maximumAcceptableTimeoutOvershoot));

            SocketChannel actualSpySocketChannel = singleResultSpyCaptor.getResult();
            assertNotNull(actualSpySocketChannel, "SocketChannel was not opened");
            verify(actualSpySocketChannel, atLeast(1)).close();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 500, 1000, 2000})
    void shouldEstablishConnection(final int connectTimeoutMs) throws IOException, InterruptedException {
        //given
        try (StreamFactoryFactory streamFactoryFactory = new TlsChannelStreamFactoryFactory(new DefaultInetAddressResolver());
             MockedStatic<SocketChannel> socketChannelMockedStatic = Mockito.mockStatic(SocketChannel.class);
             ServerSocket serverSocket = new ServerSocket(0, 1)) {

            SingleResultSpyCaptor<SocketChannel> singleResultSpyCaptor = new SingleResultSpyCaptor<>();
            socketChannelMockedStatic.when(SocketChannel::open).thenAnswer(singleResultSpyCaptor);

            StreamFactory streamFactory = streamFactoryFactory.create(SocketSettings.builder()
                    .connectTimeout(connectTimeoutMs, TimeUnit.MILLISECONDS)
                    .build(), SSL_SETTINGS);

            Stream stream = streamFactory.create(new ServerAddress(serverSocket.getInetAddress(), serverSocket.getLocalPort()));
            try {
                //when
                stream.open(createOperationContext(connectTimeoutMs));

                //then
                SocketChannel actualSpySocketChannel = singleResultSpyCaptor.getResult();
                assertNotNull(actualSpySocketChannel, "SocketChannel was not opened");
                assertTrue(actualSpySocketChannel.isConnected());

                // Wait to verify that socket was not closed by timeout.
                MILLISECONDS.sleep(connectTimeoutMs * 2L);
                assertTrue(actualSpySocketChannel.isConnected());
                assertFalse(stream.isClosed());
            } finally {
                stream.close();
            }
        }
    }

    private static final class SingleResultSpyCaptor<T> implements Answer<T> {
        private volatile T result = null;

        public T getResult() {
            return result;
        }

        @Override
        public T answer(final InvocationOnMock invocationOnMock) throws Throwable {
            if (result != null) {
                fail(invocationOnMock.getMethod().getName() + " was called more then once");
            }
            @SuppressWarnings("unchecked")
            T returnedValue = (T) invocationOnMock.callRealMethod();
            result = Mockito.spy(returnedValue);
            return result;
        }
    }

    private static OperationContext createOperationContext(final int connectTimeoutMs) {
        return simpleOperationContext(new TimeoutContext(TimeoutSettings.DEFAULT.withConnectTimeoutMS(connectTimeoutMs)));
    }

    @Test
    @DisplayName("should not call beginHandshake more than once during TLS session establishment")
    void shouldNotCallBeginHandshakeMoreThenOnceDuringTlsSessionEstablishment() throws Exception {
        assumeTrue(ClusterFixture.getSslSettings().isEnabled());

        //given
        try (StreamFactoryFactory streamFactoryFactory = new TlsChannelStreamFactoryFactory(new DefaultInetAddressResolver())) {

            SSLContext sslContext = Mockito.spy(SSLContext.getDefault());
            SingleResultSpyCaptor<SSLEngine> singleResultSpyCaptor = new SingleResultSpyCaptor<>();
            when(sslContext.createSSLEngine(anyString(), anyInt())).thenAnswer(singleResultSpyCaptor);

            StreamFactory streamFactory = streamFactoryFactory.create(
                    SocketSettings.builder().build(),
                    SslSettings.builder(ClusterFixture.getSslSettings())
                            .context(sslContext)
                            .build());

            Stream stream = streamFactory.create(getPrimaryServerDescription().getAddress());
            stream.open(ClusterFixture.OPERATION_CONTEXT);
            ByteBuf wrap = new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 3, 4}));

            //when
            stream.write(Collections.singletonList(wrap), ClusterFixture.OPERATION_CONTEXT);

            //then
            SECONDS.sleep(5);
            verify(singleResultSpyCaptor.getResult(), times(1)).beginHandshake();
        }
    }
}
