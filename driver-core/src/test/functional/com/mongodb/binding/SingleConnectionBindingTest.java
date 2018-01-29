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

package com.mongodb.binding;

import category.ReplicaSet;
import com.mongodb.connection.Connection;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static com.mongodb.ClusterFixture.getCluster;
import static com.mongodb.ReadPreference.secondary;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@Category(ReplicaSet.class)
@Ignore // Ignoring, since this is test of a test class
public class SingleConnectionBindingTest  {
    private SingleConnectionBinding binding;

    @Before
    public void setUp() {
        binding = new SingleConnectionBinding(getCluster());
    }

    @After
    public void tearDown() {
        binding.release();
    }

    @Test
    public void shouldReturnTheSameConnection() throws InterruptedException {
        ConnectionSource connectionSource = binding.getReadConnectionSource();
        Connection connection = connectionSource.getConnection();

        // Check we get the same connection
        for (int i = 0; i < 100; i++) {
            ConnectionSource connSource = binding.getReadConnectionSource();
            Connection conn = connSource.getConnection();
            assertEquals(connection.getDescription().getConnectionId(), connection.getDescription().getConnectionId());
            conn.release();
            connSource.release();
        }

        connection.release();
        connectionSource.release();
    }

    @Test
    public void shouldHaveTheSameConnectionForReadsAndWritesWithPrimaryReadPreference() throws InterruptedException {
        ConnectionSource writeSource = binding.getWriteConnectionSource();
        Connection writeConnection = writeSource.getConnection();

        ConnectionSource readSource = binding.getReadConnectionSource();
        Connection readConnection = readSource.getConnection();
        assertEquals(writeConnection.getDescription().getConnectionId(), readConnection.getDescription().getConnectionId());

        writeConnection.release();
        readConnection.release();
        writeSource.release();
        readSource.release();
    }

    @Test
    public void shouldNotDevourAllConnections() {
        for (int i = 0; i < 250; i++) {
            SingleConnectionBinding binding = new SingleConnectionBinding(getCluster());
            getAndReleaseConnectionSourceAndConnection(binding.getReadConnectionSource());
            getAndReleaseConnectionSourceAndConnection(binding.getReadConnectionSource());
            getAndReleaseConnectionSourceAndConnection(binding.getWriteConnectionSource());
            getAndReleaseConnectionSourceAndConnection(binding.getWriteConnectionSource());
            getAndReleaseConnectionSourceAndConnection(binding.getReadConnectionSource());
            getAndReleaseConnectionSourceAndConnection(binding.getReadConnectionSource());
            binding.release();
        }
    }

    @Test
    public void shouldHaveTheDifferentConnectionForReadsAndWritesWithNonPrimaryReadPreference() throws InterruptedException {
        // given
        SingleConnectionBinding binding = new SingleConnectionBinding(getCluster(), secondary());
        ConnectionSource writeSource = binding.getWriteConnectionSource();
        Connection writeConnection = writeSource.getConnection();

        ConnectionSource readSource = binding.getReadConnectionSource();
        Connection readConnection = readSource.getConnection();

        // expect
        assertThat(writeConnection.getDescription().getConnectionId(), is(not(readConnection.getDescription().getConnectionId())));

        // cleanup
        writeConnection.release();
        readConnection.release();
        writeSource.release();
        readSource.release();
        binding.release();
    }

    @Test
    public void shouldNotDevourAllConnectionsWhenUsingNonPrimaryReadPreference() {
        for (int i = 0; i < 500; i++) {
            SingleConnectionBinding binding = new SingleConnectionBinding(getCluster(), secondary());
            getAndReleaseConnectionSourceAndConnection(binding.getReadConnectionSource());
            getAndReleaseConnectionSourceAndConnection(binding.getReadConnectionSource());
            getAndReleaseConnectionSourceAndConnection(binding.getWriteConnectionSource());
            getAndReleaseConnectionSourceAndConnection(binding.getWriteConnectionSource());
            getAndReleaseConnectionSourceAndConnection(binding.getReadConnectionSource());
            getAndReleaseConnectionSourceAndConnection(binding.getReadConnectionSource());
            binding.release();
        }
    }

    private void getAndReleaseConnectionSourceAndConnection(final ConnectionSource connectionSource) {
        connectionSource.getConnection().release();
        connectionSource.release();
    }
}
