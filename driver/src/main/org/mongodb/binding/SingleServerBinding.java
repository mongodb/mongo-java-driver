package org.mongodb.binding;

import org.mongodb.ReadPreference;
import org.mongodb.connection.Cluster;
import org.mongodb.connection.Connection;
import org.mongodb.connection.ServerAddress;
import org.mongodb.selector.ServerAddressSelector;

import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * A simple binding where all connection sources are bound to the server specified in the constructor.
 *
 * @since 3.0
 */
public class SingleServerBinding extends AbstractReferenceCounted implements ReadWriteBinding {
    private final Cluster cluster;
    private final ServerAddress serverAddress;
    private final ReadPreference readPreference;
    private long maxWaitTimeMS;

    public SingleServerBinding(final Cluster cluster, final ServerAddress serverAddress, final long maxWaitTime, final TimeUnit timeUnit) {
        this(cluster, serverAddress, ReadPreference.primary(), maxWaitTime, timeUnit);
    }

    public SingleServerBinding(final Cluster cluster, final ServerAddress serverAddress, final ReadPreference readPreference,
                               final long maxWaitTime, final TimeUnit timeUnit) {
        this.cluster = cluster;
        this.serverAddress = serverAddress;
        this.readPreference = readPreference;
        this.maxWaitTimeMS = MILLISECONDS.convert(maxWaitTime, timeUnit);
    }

    @Override
    public ConnectionSource getWriteConnectionSource() {
        return new MyConnectionSource();
    }

    @Override
    public ReadPreference getReadPreference() {
        return readPreference;
    }

    @Override
    public ConnectionSource getReadConnectionSource() {
        return new MyConnectionSource();
    }

    @Override
    public SingleServerBinding retain() {
        super.retain();
        return this;
    }

    private final class MyConnectionSource extends AbstractReferenceCounted implements ConnectionSource {
        private MyConnectionSource() {
            SingleServerBinding.this.retain();
        }

        @Override
        public Connection getConnection() {
            return cluster.selectServer(new ServerAddressSelector(serverAddress), maxWaitTimeMS, MILLISECONDS).getConnection();
        }

        @Override
        public ConnectionSource retain() {
            super.retain();
            SingleServerBinding.this.retain();
            return this;
        }

        @Override
        public void release() {
            SingleServerBinding.this.release();
        }
    }
}

