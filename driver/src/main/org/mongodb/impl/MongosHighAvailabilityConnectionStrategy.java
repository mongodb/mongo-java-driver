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
import org.mongodb.ReadPreference;
import org.mongodb.ServerAddress;

import java.util.List;

public class MongosHighAvailabilityConnectionStrategy implements MongoConnectionStrategy {

    private final MongosSetMonitor mongosSetMonitor;

    public MongosHighAvailabilityConnectionStrategy(final List<ServerAddress> serverAddressList, final MongoClientOptions options) {
        mongosSetMonitor = new MongosSetMonitor(serverAddressList, options);
    }

    @Override
    public ServerAddress getAddressOfPrimary() {
        final MongosSetMember preferred = mongosSetMonitor.getCurrentState().getPreferred();
        return preferred == null ? null : preferred.getServerAddress();
    }

    @Override
    public ServerAddress getAddressForReadPreference(final ReadPreference readPreference) {
        return getAddressOfPrimary();
    }

    @Override
    public List<ServerAddress> getAllAddresses() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        mongosSetMonitor.close();
    }
}
