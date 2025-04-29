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

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.channels.InterruptedByTimeoutException;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.assertTrue;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

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
    void cleanUp() throws IOException {
        try (ServerSocket ignore = serverSocket) {
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {500, 1000, 2000})
    void shouldInterruptConnectionEstablishmentWhenConnectionTimeoutExpires(final int connectTimeout) {
        //given
        try (TlsChannelStreamFactoryFactory factory = new TlsChannelStreamFactoryFactory(new DefaultInetAddressResolver())) {
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
            long diff = elapsedMs - connectTimeout;
            // Allowed difference, with test overhead setup is 300MS.
            int epsilonMs = 300;

            assertInstanceOf(InterruptedByTimeoutException.class, mongoSocketOpenException.getCause(),
                    "Actual cause: " + mongoSocketOpenException.getCause());
            assertFalse(diff < 0,
                    String.format("Connection timed-out sooner than expected. Difference: %d ms", diff));
            assertTrue(diff < epsilonMs,
                    String.format("Elapsed time %d ms should be within %d ms of the connect timeout", elapsedMs, epsilonMs));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 500, 1000, 2000})
    void shouldEstablishConnection(final int connectTimeout) throws IOException {
        //given
        try (TlsChannelStreamFactoryFactory factory = new TlsChannelStreamFactoryFactory(new DefaultInetAddressResolver())) {
            StreamFactory streamFactory = factory.create(SocketSettings.builder()
                    .connectTimeout(connectTimeout, TimeUnit.MILLISECONDS)
                    .build(), SSL_SETTINGS);

            Stream stream = streamFactory.create(new ServerAddress("localhost", port));
            try {
                //when
                stream.open(OperationContext.simpleOperationContext(
                        new TimeoutContext(TimeoutSettings.DEFAULT.withConnectTimeoutMS(connectTimeout))));

                //then
                assertFalse(stream.isClosed());
            } finally {
                stream.close();
            }
        }
    }
}
