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

import com.mongodb.MongoClientException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ProxyProtocol;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.TimeoutSettings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import javax.net.SocketFactory;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * An HTTP proxy is supported only for KMS requests, so configuring one for connections to a MongoDB server must be
 * rejected rather than silently speaking SOCKS5 to a proxy that is not expecting it.
 */
final class HttpProxyNotSupportedForServerConnectionsTest {

    @ParameterizedTest
    @EnumSource(value = ProxyProtocol.class, names = {"HTTP", "HTTPS"})
    void shouldRejectHttpProxyForServerConnections(final ProxyProtocol protocol) {
        SocketSettings socketSettings = SocketSettings.builder()
                .applyToProxySettings(builder -> builder
                        .protocol(protocol)
                        .host("proxy.example.com")
                        .port(8080))
                .build();

        MongoClientException e = assertThrows(MongoClientException.class, () -> openStream(socketSettings));
        assertTrue(e.getMessage().contains("not supported"), () -> "unexpected message: " + e.getMessage());
        assertTrue(e.getMessage().contains(protocol.toString()), () -> "unexpected message: " + e.getMessage());
    }

    @Test
    void shouldNotRejectSocks5ProxyForServerConnections() {
        SocketSettings socketSettings = SocketSettings.builder()
                .applyToProxySettings(builder -> builder.host("proxy.example.com"))
                .build();

        // SOCKS5 is supported, so this gets as far as attempting to reach the proxy rather than being rejected outright.
        assertThrows(Exception.class, () -> openStream(socketSettings), "expected a connection failure, not a rejection");
    }

    private static void openStream(final SocketSettings socketSettings) {
        SocketStream stream = new SocketStream(new ServerAddress("cluster.example.com", 27017),
                new DefaultInetAddressResolver(), socketSettings, SslSettings.builder().build(),
                SocketFactory.getDefault(), PowerOfTwoBufferPool.DEFAULT);
        stream.open(OperationContext.simpleOperationContext(
                new TimeoutContext(TimeoutSettings.DEFAULT.withConnectTimeoutMS(10))));
    }
}
