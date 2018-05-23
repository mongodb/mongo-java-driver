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

package com.mongodb.internal.session;

import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.connection.Cluster;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.connection.ConcurrentPool;
import com.mongodb.internal.connection.ConcurrentPool.Prune;
import com.mongodb.internal.connection.NoOpSessionContext;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import com.mongodb.selector.ReadPreferenceServerSelector;
import com.mongodb.selector.ServerSelector;
import com.mongodb.session.ServerSession;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWriter;
import org.bson.UuidRepresentation;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.UuidCodec;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static com.mongodb.assertions.Assertions.isTrue;
import static java.util.concurrent.TimeUnit.MINUTES;

public class ServerSessionPool {
    private static final int END_SESSIONS_BATCH_SIZE = 10000;

    private final ConcurrentPool<ServerSessionImpl> serverSessionPool =
            new ConcurrentPool<ServerSessionImpl>(Integer.MAX_VALUE, new ServerSessionItemFactory());
    private final Cluster cluster;
    private final ServerSessionPool.Clock clock;
    private volatile boolean closing;
    private volatile boolean closed;
    private final List<BsonDocument> closedSessionIdentifiers = new ArrayList<BsonDocument>();

    interface Clock {
        long millis();
    }

    public ServerSessionPool(final Cluster cluster) {
        this(cluster, new Clock() {
            @Override
            public long millis() {
                return System.currentTimeMillis();
            }
        });
    }

    public ServerSessionPool(final Cluster cluster, final Clock clock) {
        this.cluster = cluster;
        this.clock = clock;
    }

    public ServerSession get() {
        isTrue("server session pool is open", !closed);
        ServerSessionImpl serverSession = serverSessionPool.get();
        while (shouldPrune(serverSession)) {
            serverSessionPool.release(serverSession, true);
            serverSession = serverSessionPool.get();
        }
        return serverSession;
    }

    public void release(final ServerSession serverSession) {
        serverSessionPool.release((ServerSessionImpl) serverSession);
        serverSessionPool.prune();
    }

    public void close() {
        try {
            closing = true;
            serverSessionPool.close();
            endClosedSessions();
        } finally {
            closed = true;
        }
    }

    public int getInUseCount() {
        return serverSessionPool.getInUseCount();
    }

    private void closeSession(final ServerSessionImpl serverSession) {
        serverSession.close();
        // only track closed sessions when pool is in the process of closing
        if (!closing) {
            return;
        }

        closedSessionIdentifiers.add(serverSession.getIdentifier());
        if (closedSessionIdentifiers.size() == END_SESSIONS_BATCH_SIZE) {
            endClosedSessions();
        }
    }

    private void endClosedSessions() {
        if (closedSessionIdentifiers.isEmpty()) {
            return;
        }

        final List<ServerDescription> primaryPreferred = new ReadPreferenceServerSelector(ReadPreference.primaryPreferred())
                .select(cluster.getCurrentDescription());
        if (primaryPreferred.isEmpty()) {
            return;
        }

        Connection connection = cluster.selectServer(new ServerSelector() {
            @Override
            public List<ServerDescription> select(final ClusterDescription clusterDescription) {
                for (ServerDescription cur : clusterDescription.getServerDescriptions()) {
                    if (cur.getAddress().equals(primaryPreferred.get(0).getAddress())) {
                        return Collections.singletonList(cur);
                    }
                }
                return Collections.emptyList();
            }
        }).getConnection();
        try {
            connection.command("admin",
                    new BsonDocument("endSessions", new BsonArray(closedSessionIdentifiers)), new NoOpFieldNameValidator(),
                    ReadPreference.primaryPreferred(), new BsonDocumentCodec(), NoOpSessionContext.INSTANCE);
        } catch (MongoException e) {
            // ignore exceptions
        } finally {
            closedSessionIdentifiers.clear();
            connection.release();
        }
    }

    private boolean shouldPrune(final ServerSessionImpl serverSession) {
        Integer logicalSessionTimeoutMinutes = cluster.getCurrentDescription().getLogicalSessionTimeoutMinutes();
        // if the server no longer supports sessions, prune the session
        if (logicalSessionTimeoutMinutes == null) {
            return false;
        }
        long currentTimeMillis = clock.millis();
        final long timeSinceLastUse = currentTimeMillis - serverSession.getLastUsedAtMillis();
        final long oneMinuteFromTimeout = MINUTES.toMillis(logicalSessionTimeoutMinutes - 1);
        return timeSinceLastUse > oneMinuteFromTimeout;
    }


    final class ServerSessionImpl implements ServerSession {
        private final BsonDocument identifier;
        private long transactionNumber = 0;
        private volatile long lastUsedAtMillis = clock.millis();
        private volatile boolean closed;

        ServerSessionImpl(final BsonBinary identifier) {
            this.identifier = new BsonDocument("id", identifier);
        }

        void close() {
            closed = true;
        }

        long getLastUsedAtMillis() {
            return lastUsedAtMillis;
        }

        @Override
        public long getTransactionNumber() {
            return transactionNumber;
        }

        @Override
        public BsonDocument getIdentifier() {
            lastUsedAtMillis = clock.millis();
            return identifier;
        }

        @Override
        public long advanceTransactionNumber() {
            transactionNumber++;
            return transactionNumber;
        }

        @Override
        public boolean isClosed() {
            return closed;
        }
    }

    private final class ServerSessionItemFactory implements ConcurrentPool.ItemFactory<ServerSessionImpl> {
        @Override
        public ServerSessionImpl create(final boolean initialize) {
            return new ServerSessionImpl(createNewServerSessionIdentifier());
        }

        @Override
        public void close(final ServerSessionImpl serverSession) {
            closeSession(serverSession);
        }

        @Override
        public Prune shouldPrune(final ServerSessionImpl serverSession) {
            return ServerSessionPool.this.shouldPrune(serverSession) ? Prune.YES : Prune.STOP;
        }

        private BsonBinary createNewServerSessionIdentifier() {
            UuidCodec uuidCodec = new UuidCodec(UuidRepresentation.STANDARD);
            BsonDocument holder = new BsonDocument();
            BsonDocumentWriter bsonDocumentWriter = new BsonDocumentWriter(holder);
            bsonDocumentWriter.writeStartDocument();
            bsonDocumentWriter.writeName("id");
            uuidCodec.encode(bsonDocumentWriter, UUID.randomUUID(), EncoderContext.builder().build());
            bsonDocumentWriter.writeEndDocument();
            return holder.getBinary("id");
        }
    }
}
