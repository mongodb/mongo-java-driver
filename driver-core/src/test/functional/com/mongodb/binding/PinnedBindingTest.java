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

package com.mongodb.binding;

import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ServerType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.mongodb.ClusterFixture.getCluster;
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.isSharded;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

public class PinnedBindingTest {
    private PinnedBinding binding;

    @Before
    public void setUp() {
        binding = new PinnedBinding(getCluster(), 1, SECONDS);
    }

    @After
    public void tearDown() {
        binding.release();
    }

    @Test
    public void shouldGetReadPreference() {
        assertEquals(ReadPreference.primary(), binding.getReadPreference());
        binding.setReadPreference(ReadPreference.secondary());
        assertEquals(ReadPreference.secondary(), binding.getReadPreference());
    }

    @Test
    public void shouldRetainAndRelease() {
        assertEquals(1, binding.getCount());
        binding.retain();
        assertEquals(2, binding.getCount());
        binding.release();
        assertEquals(1, binding.getCount());

        ConnectionSource source = binding.getReadConnectionSource();
        assertEquals(1, source.getCount());
        source.retain();
        assertEquals(2, source.getCount());
        source.release();
        assertEquals(1, source.getCount());
        source.release();
    }

    @Test
    public void shouldPinReadsToSameServer() throws InterruptedException {
        assumeTrue(isDiscoverableReplicaSet());

        binding.setReadPreference(ReadPreference.secondary());
        ConnectionSource readConnectionSource = binding.getReadConnectionSource();
        ServerAddress readServerAddress = readConnectionSource.getServerDescription().getAddress();
        readConnectionSource.release();

        // there is randomization in the selection, so have to try a bunch of times.
        for (int i = 0; i < 100; i++) {
            readConnectionSource = binding.getReadConnectionSource();
            assertEquals(readServerAddress, readConnectionSource.getServerDescription().getAddress());
            assertEquals(ServerType.REPLICA_SET_SECONDARY, readConnectionSource.getServerDescription().getType());
            readConnectionSource.release();
        }

        ConnectionSource writeConnectionSource = binding.getWriteConnectionSource();
        writeConnectionSource.release();

        readConnectionSource = binding.getReadConnectionSource();
        Connection connection = readConnectionSource.getConnection();
        try {
            assertEquals(readServerAddress, connection.getServerAddress());
        } finally {
            connection.release();
            readConnectionSource.release();
        }
    }

    @Test
    public void shouldUnpinWhenReadPreferenceChanges() throws InterruptedException {
        assumeTrue(isDiscoverableReplicaSet());

        binding.setReadPreference(ReadPreference.secondary());
        ConnectionSource readConnectionSource = binding.getReadConnectionSource();
        assertEquals(ServerType.REPLICA_SET_SECONDARY, readConnectionSource.getServerDescription().getType());
        readConnectionSource.release();

        binding.setReadPreference(ReadPreference.primary());
        readConnectionSource = binding.getReadConnectionSource();
        assertEquals(ServerType.REPLICA_SET_PRIMARY, readConnectionSource.getServerDescription().getType());
        readConnectionSource.release();
    }

    @Test
    public void shouldPinReadsToSameShardRouterAsWrites() {
        assumeTrue(isSharded());

        binding.setReadPreference(ReadPreference.secondary());
        ConnectionSource readConnectionSource = binding.getReadConnectionSource();
        readConnectionSource.release();

        ConnectionSource writeConnectionSource = binding.getWriteConnectionSource();
        ServerAddress writeServerAddress = writeConnectionSource.getServerDescription().getAddress();
        writeConnectionSource.release();

        readConnectionSource = binding.getReadConnectionSource();
        try {
            assertEquals(writeServerAddress, readConnectionSource.getServerDescription().getAddress());
        } finally {
            readConnectionSource.release();
        }
    }

    @Test
    public void shouldPinReadsToSameConnectionAsAPreviousWrite() throws InterruptedException {
        ConnectionSource writeSource = binding.getWriteConnectionSource();
        Connection writeConnection = writeSource.getConnection();

        ConnectionSource readSource = binding.getReadConnectionSource();
        Connection readConnection = readSource.getConnection();
        try {
            assertEquals(writeSource.getServerDescription().getAddress(), readSource.getServerDescription().getAddress());
            assertEquals(writeConnection.getId(), readConnection.getId());
        } finally {
            writeConnection.release();
            readConnection.release();
            writeSource.release();
            readSource.release();
        }
    }

    @Test
    public void shouldNotDevourAllConnections() {
        for (int i = 0; i < 250; i++) {
            PinnedBinding binding = new PinnedBinding(getCluster(), 1, SECONDS);
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
