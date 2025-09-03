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
import com.mongodb.ServerApi;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.IgnorableRequestContext;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.connection.Cluster;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.connection.NoOpSessionContext;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.selector.ReadPreferenceServerSelector;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import com.mongodb.lang.Nullable;
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
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.LongAdder;

import static com.mongodb.assertions.Assertions.isTrue;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class ServerSessionPool {
    private final ConcurrentLinkedDeque<ServerSessionImpl> available = new ConcurrentLinkedDeque<>();
    private final Cluster cluster;
    private final ServerSessionPool.Clock clock;
    private volatile boolean closed;
    private final OperationContext operationContext;
    private final LongAdder inUseCount = new LongAdder();

    interface Clock {
        long millis();
    }

    public ServerSessionPool(final Cluster cluster, final TimeoutSettings timeoutSettings, @Nullable final ServerApi serverApi) {
        this(cluster,
                new OperationContext(IgnorableRequestContext.INSTANCE, NoOpSessionContext.INSTANCE,
                        new TimeoutContext(timeoutSettings.connectionOnly()), serverApi));
    }

    public ServerSessionPool(final Cluster cluster, final OperationContext operationContext) {
        this(cluster, operationContext, System::currentTimeMillis);
    }

    public ServerSessionPool(final Cluster cluster, final OperationContext operationContext, final Clock clock) {
        this.cluster = cluster;
        this.operationContext = operationContext;
        this.clock = clock;
    }

    public ServerSession get() {
        isTrue("server session pool is open", !closed);
        ServerSessionImpl serverSession = available.pollLast();
        while (serverSession != null && shouldPrune(serverSession)) {
            serverSession.close();
            serverSession = available.pollLast();
        }
        if (serverSession == null) {
            serverSession = new ServerSessionImpl();
        }
        inUseCount.increment();
        return serverSession;
    }

    public void release(final ServerSession serverSession) {
        inUseCount.decrement();
        ServerSessionImpl serverSessionImpl = (ServerSessionImpl) serverSession;
        if (serverSessionImpl.isMarkedDirty()) {
            serverSessionImpl.close();
        } else {
            available.addLast(serverSessionImpl);
        }
    }

    public long getInUseCount() {
        return inUseCount.sum();
    }

    public void close() {
        closed = true;
        endClosedSessions();
    }

    private void endClosedSessions() {
        List<BsonDocument> identifiers = drainPool();
        if (identifiers.isEmpty()) {
            return;
        }

        ReadPreference primaryPreferred = ReadPreference.primaryPreferred();
        List<ServerDescription> primaryPreferredServers = new ReadPreferenceServerSelector(primaryPreferred)
                .select(cluster.getCurrentDescription());
        if (primaryPreferredServers.isEmpty()) {
            // Skip doing server selection if we anticipate that no server is readily selectable.
            // This approach is racy, and it is still possible to become blocked selecting a server
            // even if `primaryPreferredServers` is not empty.
            return;
        }

        Connection connection = null;
        try {
            connection = cluster.selectServer(
                    new ServerSelector() {
                        @Override
                        public List<ServerDescription> select(final ClusterDescription clusterDescription) {
                            for (ServerDescription cur : clusterDescription.getServerDescriptions()) {
                                if (cur.getAddress().equals(primaryPreferredServers.get(0).getAddress())) {
                                    return Collections.singletonList(cur);
                                }
                            }
                            return Collections.emptyList();
                        }

                        @Override
                        public String toString() {
                            return "ReadPreferenceServerSelector{"
                                    + "readPreference=" + primaryPreferred
                                    + '}';
                        }
                    },
                    operationContext).getServer().getConnection(operationContext);

            connection.command("admin",
                    new BsonDocument("endSessions", new BsonArray(identifiers)), NoOpFieldNameValidator.INSTANCE,
                    ReadPreference.primaryPreferred(), new BsonDocumentCodec(), operationContext);
        } catch (MongoException e) {
            // ignore exceptions
        } finally {
            if (connection != null) {
                connection.release();
            }
        }
    }

    /**
     * Drain the pool, returning a list of the identifiers of all drained sessions.
     */
    private List<BsonDocument> drainPool() {
        List<BsonDocument> identifiers = new ArrayList<>(available.size());
        ServerSessionImpl nextSession = available.pollFirst();
        while (nextSession != null) {
            identifiers.add(nextSession.getIdentifier());
            nextSession = available.pollFirst();
        }
        return identifiers;
    }

    private boolean shouldPrune(final ServerSessionImpl serverSession) {
        Integer logicalSessionTimeoutMinutes = cluster.getCurrentDescription().getLogicalSessionTimeoutMinutes();
        // if the server no longer supports sessions, prune the session
        if (logicalSessionTimeoutMinutes == null) {
            return false;
        }
        long currentTimeMillis = clock.millis();
        long timeSinceLastUse = currentTimeMillis - serverSession.getLastUsedAtMillis();
        long oneMinuteFromTimeout = MINUTES.toMillis(logicalSessionTimeoutMinutes - 1);
        return timeSinceLastUse > oneMinuteFromTimeout;
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

    final class ServerSessionImpl implements ServerSession {
        private final BsonDocument identifier;
        private long transactionNumber = 0;
        private volatile long lastUsedAtMillis = clock.millis();
        private volatile boolean closed;
        private volatile boolean dirty = false;

        ServerSessionImpl() {
            identifier = new BsonDocument("id", createNewServerSessionIdentifier());
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

        @Override
        public void markDirty() {
            dirty = true;
        }

        @Override
        public boolean isMarkedDirty() {
            return dirty;
        }
    }
}
