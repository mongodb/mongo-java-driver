/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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


package org.mongodb;

import org.mongodb.annotations.Immutable;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

/**
 * Represents the location of a Mongo server - i.e. server name and port number
 */
@Immutable
public class ServerAddress {
    private final String host;
    private final int port;
    private volatile InetSocketAddress address;
    /**
     * Creates a ServerAddress with default host and port
     *
     * @throws UnknownHostException
     */
    public ServerAddress()
            throws UnknownHostException {
        this(defaultHost(), defaultPort());
    }

    /**
     * Creates a ServerAddress with default port
     *
     * @param host hostname
     * @throws UnknownHostException
     */
    public ServerAddress(final String host)
            throws UnknownHostException {
        this(host, defaultPort());
    }

    /**
     * Creates a ServerAddress
     *
     * @param host hostname
     * @param port mongod port
     * @throws UnknownHostException
     */
    public ServerAddress(final String host, final int port)
            throws UnknownHostException {
        String hostToUse = host;
        if (hostToUse == null) {
            hostToUse = defaultHost();
        }
        hostToUse = hostToUse.trim();
        if (hostToUse.length() == 0) {
            hostToUse = defaultHost();
        }

        int portToUse = port;
        final int idx = hostToUse.indexOf(":");
        if (idx > 0) {
            if (port != defaultPort()) {
                throw new IllegalArgumentException("can't specify port in construct and via host");
            }
            portToUse = Integer.parseInt(hostToUse.substring(idx + 1));
            hostToUse = hostToUse.substring(0, idx).trim();
        }

        this.host = hostToUse;
        this.port = portToUse;
        updateInetAddress();
    }

    /**
     * Creates a ServerAddress with default port
     *
     * @param addr host address
     */
    public ServerAddress(final InetAddress addr) {
        this(new InetSocketAddress(addr, defaultPort()));
    }

    /**
     * Creates a ServerAddress
     *
     * @param addr host address
     * @param port mongod port
     */
    public ServerAddress(final InetAddress addr, final int port) {
        this(new InetSocketAddress(addr, port));
    }

    /**
     * Creates a ServerAddress
     *
     * @param addr inet socket address containing hostname and port
     */
    public ServerAddress(final InetSocketAddress addr) {
        address = addr;
        host = address.getHostName();
        port = address.getPort();
    }

    // --------
    // equality, etc...
    // --------


    /**
     * Determines whether this address is the same as a given host.
     *
     * @param hostName the address to compare
     * @return if they are the same
     */
    public boolean sameHost(final String hostName) {
        String hostToUse = hostName;
        final int idx = hostToUse.indexOf(":");
        int portToUse = defaultPort();
        if (idx > 0) {
            portToUse = Integer.parseInt(hostToUse.substring(idx + 1));
            hostToUse = hostToUse.substring(0, idx);
        }

        return this.port == portToUse && host.equalsIgnoreCase(hostToUse);
    }

    @Override
    public boolean equals(final Object other) {
        if (other instanceof ServerAddress) {
            final ServerAddress a = (ServerAddress) other;
            return a.port == port && a.host.equals(host);
        }
        return other instanceof InetSocketAddress && address.equals(other);
    }

    @Override
    public int hashCode() {
        return host.hashCode() + port;
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
        return address;
    }

    @Override
    public String toString() {
        return address.toString();
    }

    // --------
    // static helpers
    // --------

    /**
     * Returns the default database host: "127.0.0.1"
     *
     * @return IP address of default host.
     */
    public static String defaultHost() {
        return "127.0.0.1";
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
     * attempts to update the internal InetAddress by resolving the host name.
     *
     * @return true if host resolved to a new IP that is different from old one, false otherwise
     * @throws UnknownHostException
     */
    boolean updateInetAddress() throws UnknownHostException {
        final InetSocketAddress oldAddress = address;
        address = new InetSocketAddress(InetAddress.getByName(host), port);
        return !address.equals(oldAddress);
    }
}
