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

import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketOption;

import static com.mongodb.internal.connection.SslHelper.configureSslSocket;

@SuppressWarnings({"unchecked", "rawtypes"})
final class SocketStreamHelper {
    // Keep alive options and their values for Java 11+
    private static final String TCP_KEEPIDLE = "TCP_KEEPIDLE";
    private static final int TCP_KEEPIDLE_DURATION = 120;
    private static final String TCP_KEEPCOUNT = "TCP_KEEPCOUNT";
    private static final int TCP_KEEPCOUNT_LIMIT = 9;
    private static final String TCP_KEEPINTERVAL = "TCP_KEEPINTERVAL";
    private static final int TCP_KEEPINTERVAL_DURATION = 10;

    private static final SocketOption<Integer> KEEP_COUNT_OPTION;
    private static final SocketOption<Integer> KEEP_IDLE_OPTION;
    private static final SocketOption<Integer> KEEP_INTERVAL_OPTION;

    private static final Method SET_OPTION_METHOD;

    static {
        SocketOption<Integer> keepCountOption = null;
        SocketOption<Integer> keepIdleOption = null;
        SocketOption<Integer> keepIntervalOption = null;
        Method setOptionMethod = null;

        try {
            setOptionMethod = Socket.class.getMethod("setOption", SocketOption.class, Object.class);

            Class extendedSocketOptionsClass = Class.forName("jdk.net.ExtendedSocketOptions");
            keepCountOption = (SocketOption<Integer>) extendedSocketOptionsClass.getDeclaredField(TCP_KEEPCOUNT).get(null);
            keepIdleOption = (SocketOption<Integer>) extendedSocketOptionsClass.getDeclaredField(TCP_KEEPIDLE).get(null);
            keepIntervalOption = (SocketOption<Integer>) extendedSocketOptionsClass.getDeclaredField(TCP_KEEPINTERVAL).get(null);
        } catch (ClassNotFoundException | NoSuchMethodException | NoSuchFieldException | IllegalAccessException e) {
            // ignore: this is expected on JDKs < 11 and some deployments that don't include the jdk.net package
        }

        KEEP_COUNT_OPTION = keepCountOption;
        KEEP_IDLE_OPTION = keepIdleOption;
        KEEP_INTERVAL_OPTION = keepIntervalOption;
        SET_OPTION_METHOD = setOptionMethod;
    }

    static void initialize(final OperationContext operationContext, final Socket socket,
            final InetSocketAddress inetSocketAddress, final SocketSettings settings,
            final SslSettings sslSettings) throws IOException {
        configureSocket(socket, settings);
        configureSslSocket(socket, sslSettings, inetSocketAddress);
        socket.connect(inetSocketAddress, operationContext.getTimeoutContext().getConnectTimeoutMs());
    }

    static void configureSocket(final Socket socket, final SocketSettings settings) throws SocketException {
        socket.setTcpNoDelay(true);
        socket.setKeepAlive(true);
        // Adding keep alive options for users of Java 11+. These options will be ignored for older Java versions.
        setExtendedSocketOptions(socket);

        if (settings.getReceiveBufferSize() > 0) {
            socket.setReceiveBufferSize(settings.getReceiveBufferSize());
        }
        if (settings.getSendBufferSize() > 0) {
            socket.setSendBufferSize(settings.getSendBufferSize());
        }
    }

    static void setExtendedSocketOptions(final Socket socket) {
        try {
            if (SET_OPTION_METHOD != null) {
                if (KEEP_COUNT_OPTION != null) {
                    SET_OPTION_METHOD.invoke(socket, KEEP_COUNT_OPTION, TCP_KEEPCOUNT_LIMIT);
                }
                if (KEEP_IDLE_OPTION != null) {
                    SET_OPTION_METHOD.invoke(socket, KEEP_IDLE_OPTION, TCP_KEEPIDLE_DURATION);
                }
                if (KEEP_INTERVAL_OPTION != null) {
                    SET_OPTION_METHOD.invoke(socket, KEEP_INTERVAL_OPTION, TCP_KEEPINTERVAL_DURATION);
                }
            }
        } catch (IllegalAccessException | InvocationTargetException e) {
            // ignore failures, as this is best effort
        }
    }

    private SocketStreamHelper() {
    }
}
