/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.internal.connection;

import com.mongodb.MongoCompressor;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerSettings;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SocketStreamFactory;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import com.mongodb.selector.ServerSelector;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonString;
import org.bson.codecs.BsonDocumentCodec;
import org.junit.After;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static com.mongodb.ClusterFixture.getCredential;
import static com.mongodb.ClusterFixture.getDefaultDatabaseName;
import static com.mongodb.ClusterFixture.getPrimary;
import static com.mongodb.ClusterFixture.getSecondary;
import static com.mongodb.ClusterFixture.getSslSettings;
import static com.mongodb.internal.connection.ClusterDescriptionHelper.getPrimaries;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SingleServerClusterTest {
    private SingleServerCluster cluster;


    private void setUpCluster(final ServerAddress serverAddress) {
        SocketStreamFactory streamFactory = new SocketStreamFactory(SocketSettings.builder().build(),
                getSslSettings());
        ClusterId clusterId = new ClusterId();
        ClusterSettings clusterSettings = ClusterSettings.builder()
                .mode(ClusterConnectionMode.SINGLE)
                .hosts(singletonList(serverAddress))
                .build();
        cluster = new SingleServerCluster(clusterId,
                clusterSettings,
                new DefaultClusterableServerFactory(clusterId, clusterSettings, ServerSettings.builder().build(),
                        ConnectionPoolSettings.builder().maxSize(1).build(),
                        streamFactory, streamFactory, getCredential(),

                        null, null, null,
                        Collections.<MongoCompressor>emptyList()));
    }

    @After
    public void tearDown() {
        cluster.close();
    }

    @Test
    public void shouldGetDescription() {
        // given
        setUpCluster(getPrimary());

        // expect
        assertNotNull(cluster.getDescription());
    }

    @Test
    public void descriptionShouldIncludeSettings() {
        // given
        setUpCluster(getPrimary());

        // expect
        assertNotNull(cluster.getDescription().getClusterSettings());
        assertNotNull(cluster.getDescription().getServerSettings());
    }

    @Test
    @SuppressWarnings("deprecation")
    public void shouldGetServerWithOkDescription() {
        // given
        setUpCluster(getPrimary());

        // when
        Server server = cluster.selectServer(new ServerSelector() {
            @Override
            public List<ServerDescription> select(final ClusterDescription clusterDescription) {
                return getPrimaries(clusterDescription);
            }
        });

        // then
        assertTrue(server.getDescription().isOk());
    }

    @Test
    public void shouldSuccessfullyQueryASecondaryWithPrimaryReadPreference() {
        // given
        ServerAddress secondary = getSecondary();
        setUpCluster(secondary);
        String collectionName = getClass().getName();
        Connection connection = cluster.getServer(secondary).getConnection();

        // when
        BsonDocument result = connection.command(getDefaultDatabaseName(), new BsonDocument("count", new BsonString(collectionName)),
                new NoOpFieldNameValidator(), ReadPreference.primary(), new BsonDocumentCodec(), NoOpSessionContext.INSTANCE);

        // then
        assertEquals(new BsonDouble(1.0).intValue(), result.getNumber("ok").intValue());
    }
}
