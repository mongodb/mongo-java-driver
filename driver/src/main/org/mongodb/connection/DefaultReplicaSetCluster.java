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

package org.mongodb.connection;

import org.mongodb.MongoClientOptions;
import org.mongodb.MongoCredential;

import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

class DefaultReplicaSetCluster extends DefaultMultiServerCluster implements ReplicaSetCluster {

    public DefaultReplicaSetCluster(final List<ServerAddress> seedList, final List<MongoCredential> credentialList,
                                    final MongoClientOptions options, final BufferPool<ByteBuffer> bufferPool,
                                    final ServerFactory serverFactory) {
        super(seedList, credentialList, options, bufferPool, serverFactory);
    }

    protected void onChange(final ChangeEvent<ServerDescription> event) {
        if (isClosed()) {
            return;
        }

        synchronized (DefaultReplicaSetCluster.this) {
            if (event.getNewValue().isOk()) {
                addNewHosts(event.getNewValue().getHosts());
                addNewHosts(event.getNewValue().getPassives());
                if (event.getNewValue().isPrimary()) {
                    removeExtras(event.getNewValue());
                }
            }
            else {
                if (event.getOldValue() != null && event.getOldValue().isPrimary()) {
                    invalidateAll();
                }
            }
        }
    }

    private void addNewHosts(final List<String> hosts) {
        for (String cur : hosts) {
            ServerAddress curServerAddress = getServerAddress(cur);
            if (curServerAddress != null) {
                addServer(curServerAddress);
            }
        }
    }

    private void removeExtras(final ServerDescription serverDescription) {
        Set<ServerAddress> allServerAddresses = getAllServerAddresses(serverDescription);
        for (ServerDescription cur : getDescription().getAll()) {
            if (!allServerAddresses.contains(cur.getAddress())) {
                removeServer(cur.getAddress());
            }
        }
    }

    // TODO: move these next two methods to ServerDescription
    private Set<ServerAddress> getAllServerAddresses(final ServerDescription serverDescription) {
        Set<ServerAddress> retVal = new HashSet<ServerAddress>();
        addHostsToSet(serverDescription.getHosts(), retVal);
        addHostsToSet(serverDescription.getPassives(), retVal);
        return retVal;
    }

    private void addHostsToSet(final List<String> hosts, final Set<ServerAddress> retVal) {
        for (String host : hosts) {
            ServerAddress curServerAddress = getServerAddress(host);
            if (curServerAddress != null) {
                retVal.add(curServerAddress);
            }
        }
    }

    private ServerAddress getServerAddress(final String serverAddressString) {
        try {
            return new ServerAddress(serverAddressString);
        } catch (UnknownHostException e) {
            return null;
        }
    }
}
