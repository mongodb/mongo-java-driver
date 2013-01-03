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
 *
 */

package com.mongodb;

import org.mongodb.annotations.Immutable;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

@Immutable
public class ServerAddress {
    private final org.mongodb.ServerAddress proxied;

    public ServerAddress() throws UnknownHostException {
        proxied = new org.mongodb.ServerAddress();
    }

    public ServerAddress(final String host) throws UnknownHostException {
        proxied = new org.mongodb.ServerAddress(host);
    }

    public ServerAddress(final String host, final int port) throws UnknownHostException {
        proxied = new org.mongodb.ServerAddress(host, port);
    }

    public ServerAddress(final InetAddress addr) {
        proxied = new org.mongodb.ServerAddress(addr);
    }

    public ServerAddress(final InetAddress addr, final int port) {
        proxied = new org.mongodb.ServerAddress(addr, port);
    }

    public ServerAddress(final InetSocketAddress addr) {
        proxied = new org.mongodb.ServerAddress(addr);
    }

    public ServerAddress(final org.mongodb.ServerAddress address) {
        proxied = address;
    }

    /**
     * Gets the hostname
     * @return hostname
     */
    public String getHost(){
        return proxied.getHost();
    }

    /**
     * Gets the port number
     * @return port
     */
    public int getPort(){
        return proxied.getPort();
    }

    /**
     * Gets the underlying socket address
     * @return socket address
     */
    public InetSocketAddress getSocketAddress(){
        return proxied.getSocketAddress();
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
    public String toString(){
        return proxied.toString();
    }

    org.mongodb.ServerAddress toNew() {
        return proxied;
    }
}
