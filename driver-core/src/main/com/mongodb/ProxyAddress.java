package com.mongodb;


import com.mongodb.annotations.Immutable;

import java.io.Serializable;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;


@Immutable
public class ProxyAddress implements Serializable {
    private static final long serialVersionUID = 4027873363095395504L;

    /**
     * The host.
     */
    private final String host;
    /**
     * The port.
     */
    private final int port;

    public ProxyAddress(final String host) {
        this(host, 1080);
    }

    public ProxyAddress(final String host, final int port) {
        notNull("host", host);
        String trimmedHost = host.trim();
        isTrue("host", trimmedHost.length() > 0);
        isTrue("port", port >= 0);

        this.host = trimmedHost;
        this.port = port;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ProxyAddress that = (ProxyAddress) o;

        if (port != that.port) {
            return false;
        }

        return host.equals(that.host);
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

    @Override
    public String toString() {
        return host + ":" + port;
    }
}

