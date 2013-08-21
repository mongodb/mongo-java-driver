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
import org.mongodb.command.Command;
import org.mongodb.connection.ClusterConnectionMode;
import org.mongodb.connection.ClusterDescription;
import org.mongodb.connection.ServerAddress;
import org.mongodb.connection.ServerDescription;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.mongodb.connection.ClusterType.ReplicaSet;
import static org.mongodb.connection.ClusterType.Sharded;
import static org.mongodb.connection.ServerConnectionState.Connected;
import static org.mongodb.connection.ServerType.ReplicaSetPrimary;
import static org.mongodb.connection.ServerType.ReplicaSetSecondary;
import static org.mongodb.connection.ServerType.ShardRouter;

public class CommandReadPreferenceHelperTest {

    @Test
    public void testObedience() {
        ClusterDescription clusterDescription = new ClusterDescription(ClusterConnectionMode.Multiple, ReplicaSet, Arrays.asList(
                ServerDescription.builder().state(Connected).address(new ServerAddress()).type(ReplicaSetPrimary).build())
        );
        assertEquals(ReadPreference.secondary(), CommandReadPreferenceHelper.getCommandReadPreference(
                new Command(new Document("group", "test.test").append("key", "x")).readPreference(ReadPreference.secondary()),
                clusterDescription));
        assertEquals(ReadPreference.secondary(), CommandReadPreferenceHelper.getCommandReadPreference(
                new Command(new Document("collstats", "test.test")).readPreference(ReadPreference.secondary()),
                clusterDescription));
        assertEquals(ReadPreference.secondary(), CommandReadPreferenceHelper.getCommandReadPreference(
                new Command(new Document("dbstats", "test.test")).readPreference(ReadPreference.secondary()),
                clusterDescription));
        assertEquals(ReadPreference.secondary(), CommandReadPreferenceHelper.getCommandReadPreference(
                new Command(new Document("count", "test.test")).readPreference(ReadPreference.secondary()),
                clusterDescription));
        assertEquals(ReadPreference.secondary(), CommandReadPreferenceHelper.getCommandReadPreference(
                new Command(new Document("distinct", "test.test")).readPreference(ReadPreference.secondary()),
                clusterDescription));
        assertEquals(ReadPreference.secondary(), CommandReadPreferenceHelper.getCommandReadPreference(
                new Command(new Document("collstats", "test.test")).readPreference(ReadPreference.secondary()),
                clusterDescription));
        assertEquals(ReadPreference.secondary(), CommandReadPreferenceHelper.getCommandReadPreference(
                new Command(new Document("geoSearch", "test.test")).readPreference(ReadPreference.secondary()),
                clusterDescription));
        assertEquals(ReadPreference.secondary(), CommandReadPreferenceHelper.getCommandReadPreference(
                new Command(new Document("geoNear", "test.test")).readPreference(ReadPreference.secondary()),
                clusterDescription));
        assertEquals(ReadPreference.secondary(), CommandReadPreferenceHelper.getCommandReadPreference(
                new Command(new Document("geoWalk", "test.test")).readPreference(ReadPreference.secondary()),
                clusterDescription));
        assertEquals(ReadPreference.secondary(), CommandReadPreferenceHelper.getCommandReadPreference(
                new Command(new Document("text", "test.test")).readPreference(ReadPreference.secondary()),
                clusterDescription));
        assertEquals(ReadPreference.secondary(), CommandReadPreferenceHelper.getCommandReadPreference(
                new Command(new Document("mapReduce", "test.test").append("out", new Document("inline",
                        true))).readPreference(ReadPreference.secondary()),
                clusterDescription));

        assertEquals(ReadPreference.primary(), CommandReadPreferenceHelper.getCommandReadPreference(
                new Command(new Document("mapReduce", "test.test")).readPreference(ReadPreference.secondary()),
                clusterDescription));

        assertEquals(ReadPreference.primary(), CommandReadPreferenceHelper.getCommandReadPreference(
                new Command(new Document("shutdown", 1)).readPreference(ReadPreference.secondary()),
                clusterDescription));
    }

    @Test
    public void testIgnoreObedienceForDirectConnection() {
        ClusterDescription clusterDescription = new ClusterDescription(ClusterConnectionMode.Single, ReplicaSet,
                Arrays.asList(ServerDescription.builder().state(Connected).address(new ServerAddress()).type(ReplicaSetSecondary).build()));

        assertEquals(ReadPreference.secondary(), CommandReadPreferenceHelper.getCommandReadPreference(
                new Command(new Document("shutdown", 1)).readPreference(ReadPreference.secondary()),
                clusterDescription));
    }

    @Test
    public void testIgnoreObedienceForMongosDiscovering() {
        ClusterDescription clusterDescription = new ClusterDescription(ClusterConnectionMode.Multiple, Sharded,
                Arrays.asList(ServerDescription.builder().state(Connected).address(new ServerAddress()).type(ShardRouter).build()));

        assertEquals(ReadPreference.secondary(), CommandReadPreferenceHelper.getCommandReadPreference(
                new Command(new Document("shutdown", 1)).readPreference(ReadPreference.secondary()),
                clusterDescription));
    }
}
