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

package org.mongodb.operation;

import org.junit.Test;
import org.mongodb.Document;
import org.mongodb.ReadPreference;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.command.Command;
import org.mongodb.connection.ClusterConnectionMode;
import org.mongodb.connection.ClusterDescription;
import org.mongodb.connection.ClusterType;
import org.mongodb.connection.ServerAddress;
import org.mongodb.connection.ServerDescription;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.mongodb.Fixture.getBufferProvider;
import static org.mongodb.ReadPreference.primary;
import static org.mongodb.connection.ServerConnectionState.Connected;
import static org.mongodb.connection.ServerType.ReplicaSetPrimary;

public class AsyncCommandOperationTest {
    @Test
    public void testReadPreferenceOverride() {
        ClusterDescription clusterDescription = new ClusterDescription(ClusterConnectionMode.Multiple, ClusterType.ReplicaSet,
                Arrays.asList(ServerDescription.builder().state(Connected).address(new ServerAddress()).type(ReplicaSetPrimary).build())
        );

        AsyncCommandOperation operation = new AsyncCommandOperation("test",
                new Command(new Document("shutdown", 1)).readPreference(ReadPreference.secondary()),
                new DocumentCodec(), clusterDescription, getBufferProvider());
        assertEquals(new ReadPreferenceServerSelector(primary()), operation.getServerSelector());
    }
}