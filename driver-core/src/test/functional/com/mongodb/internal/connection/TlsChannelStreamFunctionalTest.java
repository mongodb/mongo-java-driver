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

import com.mongodb.MongoSocketOpenException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.TimeoutSettings;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.channels.InterruptedByTimeoutException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;

class TlsChannelStreamFunctionalTest {
    private static final SslSettings SSL_SETTINGS = SslSettings.builder().enabled(true).build();
    private static final String UNREACHABLE_PRIVATE_IP_ADDRESS = "10.255.255.1";
    private ServerSocket serverSocket;
    private int port;

    @BeforeEach
    void setUp() throws IOException {
        serverSocket = new ServerSocket(0, 1);
        port = serverSocket.getLocalPort();
    }

    @AfterEach
    @SuppressWarnings("try")
    void cleanUp() throws IOException {
        try (ServerSocket ignored = serverSocket) {
            //ignored
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {500, 1000, 2000})
    void shouldInterruptConnectionEstablishmentWhenConnectionTimeoutExpires(final int connectTimeout) throws IOException {
        //given
        try (TlsChannelStreamFactoryFactory factory = new TlsChannelStreamFactoryFactory(new DefaultInetAddressResolver());
             MockedStatic<SocketChannel> socketChannelMockedStatic = Mockito.mockStatic(SocketChannel.class)) {
            SingleResultSpyCaptor<SocketChannel> singleResultSpyCaptor = new SingleResultSpyCaptor<>();
            socketChannelMockedStatic.when(SocketChannel::open).thenAnswer(singleResultSpyCaptor);

            StreamFactory streamFactory = factory.create(SocketSettings.builder()
                    .connectTimeout(connectTimeout, TimeUnit.MILLISECONDS)
                    .build(), SSL_SETTINGS);

            Stream stream = streamFactory.create(new ServerAddress(UNREACHABLE_PRIVATE_IP_ADDRESS, port));
            long connectOpenStart = System.nanoTime();

            //when
            MongoSocketOpenException mongoSocketOpenException = assertThrows(MongoSocketOpenException.class, () ->
                    stream.open(OperationContext
                            .simpleOperationContext(new TimeoutContext(TimeoutSettings.DEFAULT
                                    .withConnectTimeoutMS(connectTimeout)))));

            //then
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - connectOpenStart);
            // Allow for some timing imprecision due to test overhead.
            int maximumAcceptableTimeoutOvershoot = 300;

            assertInstanceOf(InterruptedByTimeoutException.class, mongoSocketOpenException.getCause(),
                    "Actual cause: " + mongoSocketOpenException.getCause());
            assertFalse(connectTimeout > elapsedMs,
                    format("Connection timed-out sooner than expected. ConnectTimeoutMS: %d, elapsedMs: %d", connectTimeout, elapsedMs));
            assertTrue(elapsedMs - connectTimeout < maximumAcceptableTimeoutOvershoot,
                    format("Connection timeout overshoot time %d ms should be within %d ms", elapsedMs - connectTimeout,
                            maximumAcceptableTimeoutOvershoot));

            SocketChannel actualSpySocketChannel = singleResultSpyCaptor.getResult();
            assertNotNull(actualSpySocketChannel, "SocketChannel was not opened");
            verify(actualSpySocketChannel, atLeast(1)).close();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 500, 1000, 2000})
    void shouldEstablishConnection(final int connectTimeout) throws IOException, InterruptedException {
        //given
        try (TlsChannelStreamFactoryFactory factory = new TlsChannelStreamFactoryFactory(new DefaultInetAddressResolver());
             MockedStatic<SocketChannel> socketChannelMockedStatic = Mockito.mockStatic(SocketChannel.class)) {
            SingleResultSpyCaptor<SocketChannel> singleResultSpyCaptor = new SingleResultSpyCaptor<>();
            socketChannelMockedStatic.when(SocketChannel::open).thenAnswer(singleResultSpyCaptor);

            StreamFactory streamFactory = factory.create(SocketSettings.builder()
                    .connectTimeout(connectTimeout, TimeUnit.MILLISECONDS)
                    .build(), SSL_SETTINGS);

            Stream stream = streamFactory.create(new ServerAddress(serverSocket.getInetAddress(), port));
            try {
                //when
                stream.open(OperationContext.simpleOperationContext(
                        new TimeoutContext(TimeoutSettings.DEFAULT.withConnectTimeoutMS(connectTimeout))));

                //then
                SocketChannel actualSpySocketChannel = singleResultSpyCaptor.getResult();
                assertNotNull(actualSpySocketChannel, "SocketChannel was not opened");
                assertTrue(actualSpySocketChannel.isConnected());

                // Wait to verify that socket was not closed by timeout.
                SECONDS.sleep(3);
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
        public T answer(InvocationOnMock invocationOnMock) throws Throwable {
            if (result != null) {
                fail(invocationOnMock.getMethod().getName() + " was called more then once");
            }
            T returnedValue = (T) invocationOnMock.callRealMethod();
            result = Mockito.spy(returnedValue);
            return result;
        }
    }
}
