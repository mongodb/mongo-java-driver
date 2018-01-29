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
import com.mongodb.ClusterFixture;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.connection.AsyncConnection;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static com.mongodb.ClusterFixture.getAsyncCluster;
import static com.mongodb.ClusterFixture.getConnection;
import static com.mongodb.ClusterFixture.getReadConnectionSource;
import static com.mongodb.ClusterFixture.getWriteConnectionSource;
import static com.mongodb.ReadPreference.secondary;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@Category(ReplicaSet.class)
@Ignore // Ignoring, since this is test of a test class
public class AsyncSingleConnectionBindingTest  {
    private AsyncSingleConnectionBinding binding;

    @Before
    public void setUp() {
        binding = new AsyncSingleConnectionBinding(getAsyncCluster(), ClusterFixture.TIMEOUT, SECONDS);
    }

    @After
    public void tearDown() {
        binding.release();
    }

    @Test
    public void shouldReturnTheSameConnection() throws Throwable {
        AsyncConnectionSource asyncConnectionSource = getReadConnectionSource(binding);
        AsyncConnection asyncConnection = getConnection(asyncConnectionSource);

        // Check we get the same connection
        for (int i = 0; i < 100; i++) {
            AsyncConnectionSource connectionSource = getReadConnectionSource(binding);
            AsyncConnection connection = getConnection(connectionSource);
            assertEquals(connection.getDescription().getConnectionId(), asyncConnection.getDescription().getConnectionId());
            connection.release();
            connectionSource.release();
        }

        asyncConnection.release();
        asyncConnectionSource.release();
    }

    @Test
    public void shouldHaveTheSameConnectionForReadsAndWritesWithPrimaryReadPreference() throws Throwable {
        AsyncConnectionSource writeSource = getWriteConnectionSource(binding);
        AsyncConnection writeConnection = getConnection(writeSource);

        AsyncConnectionSource readSource = getReadConnectionSource(binding);
        AsyncConnection readConnection = getConnection(readSource);
        assertEquals(writeConnection.getDescription().getConnectionId(), readConnection.getDescription().getConnectionId());

        writeConnection.release();
        readConnection.release();
        writeSource.release();
        readSource.release();
    }

    @Test
    public void shouldNotDevourAllConnections() throws Throwable {
        for (int i = 0; i < 250; i++) {
            AsyncSingleConnectionBinding binding = new AsyncSingleConnectionBinding(getAsyncCluster(), ClusterFixture.TIMEOUT, SECONDS);
            getAndReleaseConnectionSourceAndConnection(getReadConnectionSource(binding));
            getAndReleaseConnectionSourceAndConnection(getReadConnectionSource(binding));
            getAndReleaseConnectionSourceAndConnection(getWriteConnectionSource(binding));
            getAndReleaseConnectionSourceAndConnection(getWriteConnectionSource(binding));
            getAndReleaseConnectionSourceAndConnection(getReadConnectionSource(binding));
            getAndReleaseConnectionSourceAndConnection(getReadConnectionSource(binding));
            binding.release();
        }
    }

    @Test
    public void shouldHaveTheDifferentConnectionForReadsAndWritesWithNonPrimaryReadPreference() throws Throwable {
        AsyncSingleConnectionBinding binding = new AsyncSingleConnectionBinding(getAsyncCluster(), secondary(), ClusterFixture.TIMEOUT,
                                                                                SECONDS);
        AsyncConnectionSource writeSource = getWriteConnectionSource(binding);
        AsyncConnection writeConnection = getConnection(writeSource);

        AsyncConnectionSource readSource = getReadConnectionSource(binding);
        AsyncConnection readConnection = getConnection(readSource);
        assertThat(writeConnection.getDescription().getConnectionId(), is(not(readConnection.getDescription().getConnectionId())));

        writeConnection.release();
        readConnection.release();
        writeSource.release();
        readSource.release();
        binding.release();
    }

    @Test
    public void shouldNotDevourAllConnectionsWhenUsingNonPrimaryReadPreference() throws Throwable {
        for (int i = 0; i < 500; i++) {
            AsyncSingleConnectionBinding binding = new AsyncSingleConnectionBinding(getAsyncCluster(), secondary(), ClusterFixture.TIMEOUT,
                                                                                    SECONDS);
            getAndReleaseConnectionSourceAndConnection(getReadConnectionSource(binding));
            getAndReleaseConnectionSourceAndConnection(getReadConnectionSource(binding));
            getAndReleaseConnectionSourceAndConnection(getWriteConnectionSource(binding));
            getAndReleaseConnectionSourceAndConnection(getWriteConnectionSource(binding));
            getAndReleaseConnectionSourceAndConnection(getReadConnectionSource(binding));
            getAndReleaseConnectionSourceAndConnection(getReadConnectionSource(binding));
            binding.release();
        }
    }

    private void getAndReleaseConnectionSourceAndConnection(final AsyncConnectionSource connectionSource) {
        connectionSource.getConnection(new SingleResultCallback<AsyncConnection>() {
            @Override
            public void onResult(final AsyncConnection connection, final Throwable t) {
                connection.release();
                connectionSource.release();
            }
        });
    }
}
