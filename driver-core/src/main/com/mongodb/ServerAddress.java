/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb;

import com.mongodb.annotations.Immutable;

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
     * Creates a ServerAddress with default host and port
     */
    public ServerAddress() {
        this(defaultHost(), defaultPort());
    }

    /**
     * Creates a ServerAddress with default port
     *
     * @param host hostname
     */
    public ServerAddress(final String host) {
        this(host, defaultPort());
    }

    /**
     * Creates a ServerAddress with default port
     *
     * @param inetAddress host address
     */
    public ServerAddress(final InetAddress inetAddress) {
        this(inetAddress.getHostName(), defaultPort());
    }

    /**
     * Creates a ServerAddress
     *
     * @param inetAddress host address
     * @param port        mongod port
     */
    public ServerAddress(final InetAddress inetAddress, final int port) {
        this(inetAddress.getHostName(), port);
    }

    /**
     * Creates a ServerAddress
     *
     * @param inetSocketAddress inet socket address containing hostname and port
     */
    public ServerAddress(final InetSocketAddress inetSocketAddress) {
        this(inetSocketAddress.getAddress(), inetSocketAddress.getPort());
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
            hostToUse = defaultHost();
        }
        hostToUse = hostToUse.trim();
        if (hostToUse.length() == 0) {
            hostToUse = defaultHost();
        }

        int portToUse = port;

        if (hostToUse.startsWith("[")) {
            int idx = host.indexOf("]");
            if (idx == -1) {
                throw new IllegalArgumentException("an IPV6 address must be encosed with '[' and ']'"
                                                   + " according to RFC 2732.");
            }

            int portIdx = host.indexOf("]:");
            if (portIdx != -1) {
                if (port != defaultPort()) {
                    throw new IllegalArgumentException("can't specify port in construct and via host");
                }
                portToUse = Integer.parseInt(host.substring(portIdx + 2));
            }
            hostToUse = host.substring(1, idx);
        } else {
            int idx = hostToUse.indexOf(":");
            int lastIdx = hostToUse.lastIndexOf(":");
            if (idx == lastIdx && idx > 0) {
                if (port != defaultPort()) {
                    throw new IllegalArgumentException("can't specify port in construct and via host");
                }
                try {
                    portToUse = Integer.parseInt(hostToUse.substring(idx + 1));
                } catch (NumberFormatException e) {
                    throw new MongoException("host and port should be specified in host:port format");
                }
                hostToUse = hostToUse.substring(0, idx).trim();
            }
        }
        this.host = hostToUse.toLowerCase();
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

        ServerAddress that = (ServerAddress) o;

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
    public InetSocketAddress getSocketAddress() {
        try {
            return new InetSocketAddress(InetAddress.getByName(host), port);
        } catch (UnknownHostException e) {
            throw new MongoSocketException(e.getMessage(), this, e);
        }
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }

    /**
     * Returns the default database host: "127.0.0.1"
     *
     * @return IP address of default host.
     */
    public static String defaultHost() {
        return "127.0.0.1"; // NOPMD
    }

    /**
     * Returns the default database port: 27017
     *
     * @return the default port
     */
    public static int defaultPort() {
        return 27017;
    }

    /**
     * Determines whether this address is the same as a given host.
     *
     * @param hostName the address to compare
     * @return if they are the same
     */
    public boolean sameHost(final String hostName) {
        String hostToUse = hostName;
        int idx = hostToUse.indexOf(":");
        int portToUse = defaultPort();
        if (idx > 0) {
            portToUse = Integer.parseInt(hostToUse.substring(idx + 1));
            hostToUse = hostToUse.substring(0, idx);
        }

        return getPort() == portToUse && getHost().equalsIgnoreCase(hostToUse);
    }
}
