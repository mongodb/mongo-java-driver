package com.mongodb;

import javax.net.SocketFactory;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.bson.util.Assertions.notNull;

final class SocketSettings {
    private final long connectTimeoutMS;
    private final long readTimeoutMS;
    private final SocketFactory socketFactory;

    public static Builder builder() {
        return new Builder();
    }
    public static class Builder {
        private long connectTimeoutMS;
        private long readTimeoutMS;
        private SocketFactory socketFactory = SocketFactory.getDefault();

        public Builder connectTimeout(final int connectTimeout, final TimeUnit timeUnit) {
            this.connectTimeoutMS = MILLISECONDS.convert(connectTimeout, timeUnit);
            return this;
        }

        public Builder readTimeout(final int readTimeout, final TimeUnit timeUnit) {
            this.readTimeoutMS = MILLISECONDS.convert(readTimeout, timeUnit);
            return this;
        }

        public Builder socketFactory(final SocketFactory socketFactory) {
            this.socketFactory = notNull("socketFactory", socketFactory);
            return this;
        }

        public SocketSettings build() {
            return new SocketSettings(this);
        }
    }

    public int getConnectTimeout(final TimeUnit timeUnit) {
        return (int) timeUnit.convert(connectTimeoutMS, MILLISECONDS);
    }

    public int getReadTimeout(final TimeUnit timeUnit) {
        return (int) timeUnit.convert(readTimeoutMS, MILLISECONDS);
    }

    public SocketFactory getSocketFactory() {
        return socketFactory;
    }

    SocketSettings(final Builder builder) {
        connectTimeoutMS = builder.connectTimeoutMS;
        readTimeoutMS = builder.readTimeoutMS;
        socketFactory = builder.socketFactory;
    }
}
