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

package org.mongodb.session;

import category.ReplicaSet;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mongodb.DatabaseTestCase;
import org.mongodb.ReadPreference;
import org.mongodb.connection.ServerAddress;
import org.mongodb.operation.Find;
import org.mongodb.operation.ReadPreferenceServerSelector;

import static org.junit.Assert.assertEquals;
import static org.mongodb.Fixture.getCluster;

@Category(ReplicaSet.class)
public class PinnedSessionTest extends DatabaseTestCase {
    private PinnedSession session;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        session = new PinnedSession(getCluster());
    }

    @Test
    public void shouldPinReadsToSameServer() throws InterruptedException {
        final Find find = new Find().readPreference(ReadPreference.secondary()).batchSize(-1);
        final ServerAddress serverAddress = session.createServerConnectionProvider(new ServerConnectionProviderOptions(true,
                new ReadPreferenceServerSelector(ReadPreference.secondary()))).getServerDescription().getAddress();
        // there is randomization in the selection, so have to try a bunch of times.
        for (int i = 0; i < 100; i++) {
            assertEquals(serverAddress, session.createServerConnectionProvider(new ServerConnectionProviderOptions(true,
                    new ReadPreferenceServerSelector(ReadPreference.secondary()))).getServerDescription().getAddress());
        }

        session.createServerConnectionProvider(new ServerConnectionProviderOptions(false, new PrimaryServerSelector()));

        assertEquals(serverAddress, session.createServerConnectionProvider(new ServerConnectionProviderOptions(true,
                new ReadPreferenceServerSelector(ReadPreference.secondary()))).getServerDescription().getAddress());
    }
}
