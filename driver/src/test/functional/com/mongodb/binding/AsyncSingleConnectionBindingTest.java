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

import category.ReplicaSet;
import com.mongodb.connection.Connection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static com.mongodb.ClusterFixture.getAsyncCluster;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

@Category(ReplicaSet.class)
public class AsyncSingleConnectionBindingTest  {
    private AsyncSingleConnectionBinding binding;

    @Before
    public void setUp() {
        binding = new AsyncSingleConnectionBinding(getAsyncCluster(), 1, SECONDS);
    }

    @After
    public void tearDown() {
        binding.release();
    }

    @Test
    public void shouldReturnTheSameConnection() throws InterruptedException {
        AsyncConnectionSource asyncConnectionSource = binding.getReadConnectionSource().get();
        Connection asyncConnection = asyncConnectionSource.getConnection().get();

        // Check we get the same connection
        for (int i = 0; i < 100; i++) {
            AsyncConnectionSource connectionSource = binding.getReadConnectionSource().get();
            Connection connection = connectionSource.getConnection().get();
            assertEquals(connection.getId(), asyncConnection.getId());
            connection.release();
            connectionSource.release();
        }

        asyncConnection.release();
        asyncConnectionSource.release();
    }

    @Test
    public void shouldHaveTheSameConnectionForReadsAndWrites() throws InterruptedException {
        AsyncConnectionSource writeSource = binding.getWriteConnectionSource().get();
        Connection writeConnection = writeSource.getConnection().get();

        AsyncConnectionSource readSource = binding.getReadConnectionSource().get();
        Connection readConnection = readSource.getConnection().get();
        assertEquals(writeConnection.getId(), readConnection.getId());

        writeConnection.release();
        readConnection.release();
        writeSource.release();
        readSource.release();
    }

    @Test
    public void shouldNotDevourAllConnections() {
        for (int i = 0; i < 250; i++) {
            AsyncSingleConnectionBinding binding = new AsyncSingleConnectionBinding(getAsyncCluster(), 1, SECONDS);
            getAndReleaseConnectionSourceAndConnection(binding.getReadConnectionSource().get());
            getAndReleaseConnectionSourceAndConnection(binding.getReadConnectionSource().get());
            getAndReleaseConnectionSourceAndConnection(binding.getWriteConnectionSource().get());
            getAndReleaseConnectionSourceAndConnection(binding.getWriteConnectionSource().get());
            getAndReleaseConnectionSourceAndConnection(binding.getReadConnectionSource().get());
            getAndReleaseConnectionSourceAndConnection(binding.getReadConnectionSource().get());
            binding.release();
        }
    }

    private void getAndReleaseConnectionSourceAndConnection(final AsyncConnectionSource connectionSource) {
        connectionSource.getConnection().get().release();
        connectionSource.release();
    }
}
