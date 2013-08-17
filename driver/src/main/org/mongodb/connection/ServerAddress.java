/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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


package org.mongodb.connection;

import org.mongodb.annotations.Immutable;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

/**
 * Represents the location of a Mongo server - i.e. server name and port number
 */
@Immutable
public class ServerAddress implements Serializable {

    private static final long serialVersionUID = 4027873363095395504L;

    private final String host;
    private final int port;

    /**
     * Returns the default database host: "127.0.0.1"
     *
     * @return IP address of default host.
     */
    public static String getDefaultHost() {
        return "127.0.0.1"; // NOPMD
    }

    /**
     * Returns the default database port: 27017
     *
     * @return the default port
     */
    public static int getDefaultPort() {
        return 27017;
    }

    /**
     * Creates a ServerAddress with default host and port
     *
     */
    public ServerAddress() {
        this(getDefaultHost(), getDefaultPort());
    }

    /**
     * Creates a ServerAddress with default port
     *
     * @param host hostname
     */
    public ServerAddress(final String host) {
        this(host, getDefaultPort());
    }

    /**
     * Creates a ServerAddress
     *
     * @param host hostname
     * @param port mongod port
     */
    public ServerAddress(final String host, final int port) {
        String hostToUse = host;
        if (hostToUse == null) {
            hostToUse = getDefaultHost();
        }
        hostToUse = hostToUse.trim();
        if (hostToUse.length() == 0) {
            hostToUse = getDefaultHost();
        }

        int portToUse = port;
        final int idx = hostToUse.indexOf(":");
        if (idx > 0) {
            if (port != getDefaultPort()) {
                throw new IllegalArgumentException("can't specify port in construct and via host");
            }
            portToUse = Integer.parseInt(hostToUse.substring(idx + 1));
            hostToUse = hostToUse.substring(0, idx).trim();
        }

        this.host = hostToUse;
        this.port = portToUse;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final ServerAddress that = (ServerAddress) o;

        if (port != that.port) {
            return false;
        }

        if (!host.equals(that.host)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = host.hashCode();
        result = 31 * result + port;
        return result;
    }

    /**
     * Gets the hostname
     *
     * @return hostname
     */
    public String getHost() {
        return host;
    }

    /**
     * Gets the port number
     *
     * @return port
     */
    public int getPort() {
        return port;
    }

    /**
     * Gets the underlying socket address
     *
     * @return socket address
     */
    public InetSocketAddress getSocketAddress() throws UnknownHostException {
        return new InetSocketAddress(InetAddress.getByName(host), port);
    }


    @Override
    public String toString() {
        return host + ":" + port;
    }
}
