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

package org.mongodb.impl;

import org.mongodb.MongoClientOptions;
import org.mongodb.MongoConnectionStrategy;
import org.mongodb.MongoNoPrimaryException;
import org.mongodb.MongoReadPreferenceException;
import org.mongodb.ReadPreference;
import org.mongodb.ServerAddress;
import org.mongodb.io.BufferPool;
import org.mongodb.rs.ReplicaSet;
import org.mongodb.rs.ReplicaSetMember;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

import static org.mongodb.assertions.Assertions.notNull;

public class ReplicaSetConnectionStrategy implements MongoConnectionStrategy {
    private final ReplicaSetMonitor replicaSetMonitor;

    public ReplicaSetConnectionStrategy(final List<ServerAddress> seedList, final MongoClientOptions options,
                                        final BufferPool<ByteBuffer> bufferPool) {
        notNull("seedList", seedList);
        notNull("options", options);
        notNull("bufferPool", bufferPool);

        replicaSetMonitor = new ReplicaSetMonitor(seedList);

        replicaSetMonitor.start(new DefaultMongoServerStateNotifierFactory(bufferPool, options, replicaSetMonitor),
                Executors.newScheduledThreadPool(seedList.size()));
    }

    @Override
    public ServerAddress getAddressOfPrimary() {
        ReplicaSet currentState = replicaSetMonitor.getCurrentState();
        ReplicaSetMember primary = currentState.getPrimary();
        if (primary == null) {
            throw new MongoNoPrimaryException(currentState);
        }
        return primary.getServerAddress();
    }

    @Override
    public ServerAddress getAddressForReadPreference(final ReadPreference readPreference) {
        // TODO: this is hiding potential bugs.  ReadPreference should not be null
        ReadPreference appliedReadPreference = readPreference == null ? ReadPreference.primary() : readPreference;
        final ReplicaSet replicaSet = replicaSetMonitor.getCurrentState();
        final ReplicaSetMember replicaSetMember = appliedReadPreference.chooseReplicaSetMember(replicaSet);
        if (replicaSetMember == null) {
            throw new MongoReadPreferenceException(readPreference, replicaSet);
        }
        return replicaSetMember.getServerAddress();
    }

    @Override
    public List<ServerAddress> getAllAddresses() {
        List<ServerAddress> addressList = new ArrayList<ServerAddress>();
        ReplicaSet currentState = replicaSetMonitor.getCurrentState();
        for (ReplicaSetMember cur : currentState.getAll()) {
             addressList.add(cur.getAddress());
        }
        return addressList;
    }

    @Override
    public void close() {
        replicaSetMonitor.shutdownNow();
    }
}