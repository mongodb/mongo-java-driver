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

package com.mongodb.internal.connection

import com.mongodb.ClusterFixture
import com.mongodb.MongoInternalException
import com.mongodb.connection.SocketSettings
import com.mongodb.connection.SslSettings
import jdk.net.ExtendedSocketOptions
import spock.lang.IgnoreIf
import spock.lang.Specification

import javax.net.SocketFactory
import javax.net.ssl.SNIHostName
import javax.net.ssl.SSLSocket
import javax.net.ssl.SSLSocketFactory
import java.lang.reflect.Method

import static com.mongodb.ClusterFixture.getPrimary
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static java.util.concurrent.TimeUnit.SECONDS

class SocketStreamHelperSpecification extends Specification {

    def 'should configure socket with settings()'() {
        given:
        Socket socket = SocketFactory.default.createSocket()
        def socketSettings = SocketSettings.builder()
                .readTimeout(10, SECONDS)
                .build()

        when:
        SocketStreamHelper.initialize(socket, getPrimary().getSocketAddress(), socketSettings, SslSettings.builder().build())

        then:
        socket.getTcpNoDelay()
        socket.getKeepAlive()
        socket.getSoTimeout() == socketSettings.getReadTimeout(MILLISECONDS)

        // If the Java 11+ extended socket options for keep alive probes are available, check those values.
        if (Arrays.stream(ExtendedSocketOptions.getDeclaredFields()).anyMatch{ f -> f.getName().equals('TCP_KEEPCOUNT') }) {
            Method getOptionMethod = Socket.getMethod('getOption', SocketOption);
            getOptionMethod.invoke(socket, ExtendedSocketOptions.getDeclaredField('TCP_KEEPCOUNT').get(null)) == 9
            getOptionMethod.invoke(socket, ExtendedSocketOptions.getDeclaredField('TCP_KEEPIDLE').get(null)) == 120
            getOptionMethod.invoke(socket, ExtendedSocketOptions.getDeclaredField('TCP_KEEPINTERVAL').get(null)) == 10
        }

        cleanup:
        socket?.close()
    }

    def 'should connect socket()'() {
        given:
        Socket socket = SocketFactory.default.createSocket()

        when:
        SocketStreamHelper.initialize(socket, getPrimary().getSocketAddress(),
                SocketSettings.builder().build(), SslSettings.builder().build())

        then:
        socket.isConnected()

        cleanup:
        socket?.close()
    }

    @IgnoreIf({ !ClusterFixture.sslSettings.enabled })
    def 'should enable host name verification if socket is an instance of SSLSocket'() {
        given:
        SSLSocket socket = SSLSocketFactory.default.createSocket()

        when:
        SocketStreamHelper.initialize(socket, getPrimary().getSocketAddress(), SocketSettings.builder().build(), sslSettings)

        then:
        socket.getSSLParameters().endpointIdentificationAlgorithm == (sslSettings.invalidHostNameAllowed ? null : 'HTTPS')

        cleanup:
        socket?.close()

        where:
        sslSettings << [SslSettings.builder().enabled(true).build(),
                        SslSettings.builder().enabled(false).build(),
                        SslSettings.builder().enabled(true).invalidHostNameAllowed(true).build()]
    }

    @IgnoreIf({ !ClusterFixture.sslSettings.enabled })
    def 'should enable SNI if socket is an instance of SSLSocket'() {
        given:
        SSLSocket socket = SSLSocketFactory.default.createSocket()

        when:
        SocketStreamHelper.initialize(socket, getPrimary().getSocketAddress(), SocketSettings.builder().build(), sslSettings)

        then:
        socket.getSSLParameters().getServerNames() == [new SNIHostName(getPrimary().getHost())]

        cleanup:
        socket?.close()

        where:
        sslSettings << [SslSettings.builder().enabled(true).build(),
                        SslSettings.builder().enabled(false).build()]
    }

    def 'should throw MongoInternalException is ssl is enabled and the socket is not an instance of SSLSocket'() {
        given:
        Socket socket = SocketFactory.default.createSocket()

        when:
        SocketStreamHelper.initialize(socket, getPrimary().getSocketAddress(), SocketSettings.builder().build(),
                SslSettings.builder().enabled(true).build())

        then:
        thrown(MongoInternalException)

        cleanup:
        socket?.close()
    }
}
