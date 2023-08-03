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

import com.mongodb.MongoInternalException;
import com.mongodb.connection.SslSettings;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SNIServerName;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;

import java.net.InetSocketAddress;
import java.net.Socket;

import static java.util.Collections.singletonList;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class SslHelper {

    /**
     * Enable HTTP endpoint verification on the given SSL parameters.
     *
     * @param sslParameters The original SSL parameters
     */
    public static void enableHostNameVerification(final SSLParameters sslParameters) {
        sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
    }

    /**
     * Enable SNI.
     *
     * @param host          the server host
     * @param sslParameters the SSL parameters
     */
    public static void enableSni(final String host, final SSLParameters sslParameters) {
        try {
            SNIServerName sniHostName = new SNIHostName(host);
            sslParameters.setServerNames(singletonList(sniHostName));
        } catch (IllegalArgumentException e) {
            // ignore because SNIHostName will throw this for some legit host names for connecting to MongoDB, e.g an IPV6 literal
        }
    }

    public static void configureSslSocket(final Socket socket, final SslSettings sslSettings, final InetSocketAddress inetSocketAddress) throws
            MongoInternalException {
        if (sslSettings.isEnabled() || socket instanceof SSLSocket) {
            if (!(socket instanceof SSLSocket)) {
                throw new MongoInternalException("SSL is enabled but the socket is not an instance of javax.net.ssl.SSLSocket");
            }
            SSLSocket sslSocket = (SSLSocket) socket;
            SSLParameters sslParameters = sslSocket.getSSLParameters();
            if (sslParameters == null) {
                sslParameters = new SSLParameters();
            }

            enableSni(inetSocketAddress.getHostName(), sslParameters);

            if (!sslSettings.isInvalidHostNameAllowed()) {
                enableHostNameVerification(sslParameters);
            }
            sslSocket.setSSLParameters(sslParameters);
        }
    }

    private SslHelper() {
    }
}
