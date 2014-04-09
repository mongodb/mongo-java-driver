/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package org.mongodb.session;

import category.ReplicaSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mongodb.DatabaseTestCase;
import org.mongodb.connection.ServerAddress;
import org.mongodb.selector.ReadPreferenceServerSelector;
import org.mongodb.selector.PrimaryServerSelector;

import static org.junit.Assert.assertEquals;
import static org.mongodb.Fixture.getCluster;
import static org.mongodb.ReadPreference.primary;
import static org.mongodb.ReadPreference.secondary;

@Category(ReplicaSet.class)
public class PinnedSessionTest extends DatabaseTestCase {
    private PinnedSession session;

    @Before
    public void setUp() {
        super.setUp();
        session = new PinnedSession(getCluster());
    }

    @After
    public void tearDown() {
        session.close();
    }

    @Test
    public void shouldPinReadsToSameServer() throws InterruptedException {
        ServerConnectionProviderOptions options = new ServerConnectionProviderOptions(true, new ReadPreferenceServerSelector(secondary()));

        ServerAddress serverAddress = session.createServerConnectionProvider(options)
                                             .getServerDescription()
                                             .getAddress();
        // there is randomization in the selection, so have to try a bunch of times.
        for (int i = 0; i < 100; i++) {
            assertEquals(serverAddress, session.createServerConnectionProvider(options)
                                               .getServerDescription()
                                               .getAddress());
        }

        session.createServerConnectionProvider(new ServerConnectionProviderOptions(false, new PrimaryServerSelector()));

        assertEquals(serverAddress, session.createServerConnectionProvider(options)
                                           .getServerDescription()
                                           .getAddress());
    }

    @Test
    public void shouldPinReadsToSameConnectionAsAPreviousWrite() throws InterruptedException {
        ServerConnectionProviderOptions writeOptions = new ServerConnectionProviderOptions(false, new PrimaryServerSelector());
        ServerConnectionProvider writeProvider = session.createServerConnectionProvider(writeOptions);
        String connectionId = writeProvider.getConnection().getId();

        ServerConnectionProviderOptions readOptions = new ServerConnectionProviderOptions(true,
                                                                                          new ReadPreferenceServerSelector(primary()));
        ServerConnectionProvider readProvider = session.createServerConnectionProvider(readOptions);
        assertEquals(connectionId, readProvider.getConnection().getId());
    }
}
