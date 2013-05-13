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
import org.mongodb.MongoCredential;
import org.mongodb.MongoNoPrimaryException;
import org.mongodb.MongoReadPreferenceException;
import org.mongodb.MongoServer;
import org.mongodb.ReadPreference;
import org.mongodb.ServerAddress;
import org.mongodb.io.BufferPool;
import org.mongodb.rs.ReplicaSetDescription;
import org.mongodb.rs.ReplicaSetMemberDescription;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

import static org.mongodb.assertions.Assertions.isTrue;
import static org.mongodb.assertions.Assertions.notNull;

public class ReplicaSetServerBinding extends MongoMultiServerBinding {
    private final ReplicaSetMonitor replicaSetMonitor;

    public ReplicaSetServerBinding(final List<ServerAddress> seedList, final List<MongoCredential> credentialList,
                                   final MongoClientOptions options, final BufferPool<ByteBuffer> bufferPool) {
        super(credentialList, options, bufferPool);
        notNull("seedList", seedList);
        notNull("options", options);
        notNull("bufferPool", bufferPool);

        replicaSetMonitor = new ReplicaSetMonitor(seedList);

        replicaSetMonitor.start(new DefaultMongoServerStateNotifierFactory(bufferPool, options, replicaSetMonitor),
                Executors.newScheduledThreadPool(seedList.size()));
    }

    @Override
    public MongoServer getConnectionManagerForWrite() {
        isTrue("open", !isClosed());

        return getConnectionManagerForServer(getAddressOfPrimary());
    }

    @Override
    public MongoServer getConnectionManagerForRead(final ReadPreference readPreference) {
        isTrue("open", !isClosed());

        return getConnectionManagerForServer(getAddressForReadPreference(readPreference));
    }

    @Override
    public List<ServerAddress> getAllServerAddresses() {
        isTrue("open", !isClosed());

        List<ServerAddress> addressList = new ArrayList<ServerAddress>();
        ReplicaSetDescription currentReplicaSetDescription = replicaSetMonitor.getCurrentReplicaSetDescription();
        for (ReplicaSetMemberDescription cur : currentReplicaSetDescription.getAll()) {
            addressList.add(cur.getAddress());
        }
        return addressList;
    }

    @Override
    public void close() {
        replicaSetMonitor.shutdownNow();
        super.close();
    }

    private ServerAddress getAddressOfPrimary() {
        ReplicaSetDescription currentDescription = replicaSetMonitor.getCurrentReplicaSetDescription();
        ReplicaSetMemberDescription primary = currentDescription.getPrimary();
        if (primary == null) {
            throw new MongoNoPrimaryException(currentDescription);
        }
        return primary.getServerAddress();
    }

    private ServerAddress getAddressForReadPreference(final ReadPreference readPreference) {
        // TODO: this is hiding potential bugs.  ReadPreference should not be null
        ReadPreference appliedReadPreference = readPreference == null ? ReadPreference.primary() : readPreference;
        final ReplicaSetDescription replicaSetDescription = replicaSetMonitor.getCurrentReplicaSetDescription();
        final ReplicaSetMemberDescription replicaSetMemberDescription = appliedReadPreference.chooseReplicaSetMember(replicaSetDescription);
        if (replicaSetMemberDescription == null) {
            throw new MongoReadPreferenceException(readPreference, replicaSetDescription);
        }
        return replicaSetMemberDescription.getServerAddress();
    }
}