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

import org.mongodb.annotations.Immutable;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

@Immutable
public class ServerAddress {
    private final org.mongodb.connection.ServerAddress proxied;

    /**
     * Returns the default database host: "127.0.0.1"
     *
     * @return IP address of default host.
     */
    public static String defaultHost() {
        return org.mongodb.connection.ServerAddress.getDefaultHost();
    }

    /**
     * Returns the default database port: 27017
     *
     * @return the default port
     */
    public static int defaultPort() {
        return org.mongodb.connection.ServerAddress.getDefaultPort();
    }

    /**
     * Creates a ServerAddress with default host and port
     * @throws UnknownHostException
     */
    public ServerAddress() throws UnknownHostException {
        proxied = new org.mongodb.connection.ServerAddress();
    }

    /**
     * Creates a ServerAddress with default port.
     * If the host parameter contains a port, i.e. 127.0.0.1:3333
     * it will use that port instead of the default one.
     * @param host hostname
     * @throws UnknownHostException
     */
    public ServerAddress(final String host) throws UnknownHostException {
        proxied = new org.mongodb.connection.ServerAddress(host);
    }

    /**
     * Creates a ServerAddress
     * @param host hostname
     * @param port mongod port
     * @throws UnknownHostException
     */
    public ServerAddress(final String host, final int port) {
        proxied = new org.mongodb.connection.ServerAddress(host, port);
    }

    /**
     * Creates a ServerAddress with default port
     * @param inetAddress host address
     */
    public ServerAddress(final InetAddress inetAddress) {
        this(inetAddress.getHostName(), defaultPort());
    }

    /**
     * Creates a ServerAddress
     * @param inetAddress host address
     * @param port mongod port
     */
    public ServerAddress(final InetAddress inetAddress, final int port) {
        this(inetAddress.getHostName(), port);
    }

    /**
     * Creates a ServerAddress
     * @param inetSocketAddress inet socket address containing hostname and port
     */
    public ServerAddress(final InetSocketAddress inetSocketAddress) {
        this(inetSocketAddress.getAddress(), inetSocketAddress.getPort());
    }

    public ServerAddress(final org.mongodb.connection.ServerAddress address) {
        proxied = address;
    }

    /**
     * Gets the hostname
     *
     * @return hostname
     */
    public String getHost() {
        return proxied.getHost();
    }

    /**
     * Gets the port number
     *
     * @return port
     */
    public int getPort() {
        return proxied.getPort();
    }

    /**
     * Gets the underlying socket address
     *
     * @return socket address
     */
    public InetSocketAddress getSocketAddress() {
        try {
            return proxied.getSocketAddress();
        } catch (UnknownHostException e) {
            throw new MongoException("Unknown host", e);
        }
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        ServerAddress that = (ServerAddress) other;

        if (!proxied.equals(that.proxied)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return proxied.hashCode();
    }

    @Override
    public String toString() {
        return proxied.toString();
    }

    org.mongodb.connection.ServerAddress toNew() {
        return proxied;
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
