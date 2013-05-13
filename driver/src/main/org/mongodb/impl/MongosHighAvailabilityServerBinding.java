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
import org.mongodb.MongoServer;
import org.mongodb.ReadPreference;
import org.mongodb.ServerAddress;
import org.mongodb.io.BufferPool;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class MongosHighAvailabilityServerBinding extends MongoMultiServerBinding {

    private final MongosSetMonitor mongosSetMonitor;

    public MongosHighAvailabilityServerBinding(final List<ServerAddress> seedList, final List<MongoCredential> credentialList,
                                               final MongoClientOptions options, final BufferPool<ByteBuffer> bufferPool) {
        super(credentialList, options, bufferPool);
        mongosSetMonitor = new MongosSetMonitor(seedList, options);
    }

    @Override
    public MongoServer getConnectionManagerForWrite() {
        return getConnectionManagerForServer(getPreferred());
    }

    @Override
    public MongoServer getConnectionManagerForRead(final ReadPreference readPreference) {
        return getConnectionManagerForServer(getPreferred());
    }

    @Override
    public List<ServerAddress> getAllServerAddresses() {
        return Arrays.asList(getPreferred());
    }

    @Override
    public void close() {
        mongosSetMonitor.close();
        super.close();
    }

    private ServerAddress getPreferred() {
        final MongosSetMemberDescription preferred = mongosSetMonitor.getCurrentState().getPreferred();
        return preferred == null ? null : preferred.getServerAddress();
    }
}
