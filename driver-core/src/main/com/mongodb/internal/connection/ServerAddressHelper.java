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


import com.mongodb.ServerAddress;
import com.mongodb.UnixServerAddress;

public final class ServerAddressHelper {

    public static ServerAddress createServerAddress(final String host) {
        return createServerAddress(host, ServerAddress.defaultPort());
    }

    public static ServerAddress createServerAddress(final String host, final int port) {
        if (host != null && host.endsWith(".sock")) {
            return new UnixServerAddress(host);
        } else {
            return new ServerAddress(host, port);
        }
    }

    /**
     * @param address Either a host as in {@link ServerAddress#ServerAddress(String)} or both a host and a port separated with {@code ':'}.
     */
    public static ServerAddress parse(final String address) {
        String[] parts = address.split(":", 2);
        if (parts.length == 1) {
            // `address` contains only a host
            return new ServerAddress(address);
        } else {
            return new ServerAddress(parts[0], Integer.parseInt(parts[1]));
        }
    }

    private ServerAddressHelper() {
    }
}
