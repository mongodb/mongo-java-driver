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
import org.mongodb.connection.ClusterDescription;
import org.mongodb.connection.ServerAddress;
import org.mongodb.connection.ServerDescription;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mongodb.ReadPreference.primary;
import static org.mongodb.ReadPreference.secondary;
import static org.mongodb.connection.ClusterConnectionMode.Multiple;
import static org.mongodb.connection.ClusterConnectionMode.Single;
import static org.mongodb.connection.ClusterType.ReplicaSet;
import static org.mongodb.connection.ClusterType.Sharded;
import static org.mongodb.connection.ServerConnectionState.Connected;
import static org.mongodb.connection.ServerType.ReplicaSetPrimary;
import static org.mongodb.connection.ServerType.ReplicaSetSecondary;
import static org.mongodb.connection.ServerType.ShardRouter;
import static org.mongodb.operation.CommandReadPreferenceHelper.getCommandReadPreference;

public class CommandReadPreferenceHelperTest {

    @Test
    public void testObedience() {
        ClusterDescription clusterDescription = new ClusterDescription(Multiple,
                                                                       ReplicaSet,
                                                                       asList(ServerDescription.builder()
                                                                                               .state(Connected)
                                                                                               .address(new ServerAddress())
                                                                                               .type(ReplicaSetPrimary)
                                                                                               .build()));
        assertEquals(secondary(), getCommandReadPreference(new Document("group", "test.test").append("key", "x"), secondary(),
                                                           clusterDescription));
        assertEquals(secondary(), getCommandReadPreference(new Document("collstats", "test.test"), secondary(),
                                                           clusterDescription));
        assertEquals(secondary(), getCommandReadPreference(new Document("dbstats", "test.test"), secondary(),
                                                           clusterDescription));
        assertEquals(secondary(), getCommandReadPreference(new Document("count", "test.test"), secondary(),
                                                           clusterDescription));
        assertEquals(secondary(), getCommandReadPreference(new Document("distinct", "test.test"), secondary(),
                                                           clusterDescription));
        assertEquals(secondary(), getCommandReadPreference(new Document("collstats", "test.test"), secondary(),
                                                           clusterDescription));
        assertEquals(secondary(), getCommandReadPreference(new Document("geoSearch", "test.test"), secondary(),
                                                           clusterDescription));
        assertEquals(secondary(), getCommandReadPreference(new Document("geoNear", "test.test"), secondary(),
                                                           clusterDescription));
        assertEquals(secondary(), getCommandReadPreference(new Document("geoWalk", "test.test"), secondary(),
                                                           clusterDescription));
        assertEquals(secondary(), getCommandReadPreference(new Document("text", "test.test"), secondary(),
                                                           clusterDescription));
        assertEquals(secondary(), getCommandReadPreference(new Document("mapReduce", "test.test")
                                                           .append("out", new Document("inline", true)),
                                                           secondary(),
                                                           clusterDescription));

        assertEquals(primary(), getCommandReadPreference(new Document("mapReduce", "test.test"), secondary(),
                                                         clusterDescription));

        assertEquals(primary(), getCommandReadPreference(new Document("shutdown", 1), secondary(),
                                                         clusterDescription));
    }

    @Test
    public void testIgnoreObedienceForDirectConnection() {
        ClusterDescription clusterDescription = new ClusterDescription(Single, ReplicaSet,
                                                                       asList(ServerDescription.builder()
                                                                                               .state(Connected)
                                                                                               .address(new ServerAddress())
                                                                                               .type(ReplicaSetSecondary).build()));

        assertEquals(secondary(), getCommandReadPreference(new Document("shutdown", 1), secondary(), clusterDescription));
    }

    @Test
    public void testIgnoreObedienceForMongosDiscovering() {
        ClusterDescription clusterDescription = new ClusterDescription(Multiple, Sharded,
                                                                       asList(ServerDescription.builder()
                                                                                               .state(Connected)
                                                                                               .address(new ServerAddress())
                                                                                               .type(ShardRouter).build()));

        assertEquals(secondary(), getCommandReadPreference(new Document("shutdown", 1), secondary(), clusterDescription));
    }
}
