/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.mongodb.internal.connection;

import com.mongodb.ServerAddress;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SNIServerName;
import javax.net.ssl.SSLParameters;

import static java.util.Collections.singletonList;

// This class is loaded via Class.forName from SslHelper.
final class Java8SniSslHelper implements SniSslHelper {

    // if running on Java 8 or above then SNIHostName will be available and initialization will succeed.  Otherwise it will fail.
    static {
        try {
            Class.forName("javax.net.ssl.SNIHostName");
        } catch (ClassNotFoundException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Override
    public void enableSni(final ServerAddress address, final SSLParameters sslParameters) {
        try {
            SNIServerName sniHostName = new SNIHostName(address.getHost());
            sslParameters.setServerNames(singletonList(sniHostName));
        } catch (IllegalArgumentException e) {
            // ignore because SNIHostName will throw this for some legit host names for connecting to MongoDB, e.g an IPV6 literal
        }
    }

    Java8SniSslHelper() {
    }
}
