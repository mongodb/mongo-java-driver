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
package com.mongodb.kotlin.client.coroutine

import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import com.mongodb.MongoDriverInformation
import com.mongodb.connection.ClusterDescription
import com.mongodb.lang.Nullable
import com.mongodb.reactivestreams.client.MongoClient as JMongoClient
import com.mongodb.reactivestreams.client.MongoClients as JMongoClients
import java.io.Closeable

/**
 * A client-side representation of a MongoDB cluster.
 *
 * Instances can represent either a standalone MongoDB instance, a replica set, or a sharded cluster. Instance of this
 * class are responsible for maintaining an up-to-date state of the cluster, and possibly cache resources related to
 * this, including background threads for monitoring, and connection pools.
 *
 * Instances of this class serve as factories for [MongoDatabase] instances. Instances of this class can be created via
 * the [MongoClient.create] helpers
 *
 * @see MongoClient.create
 */
public class MongoClient(private val wrapped: JMongoClient) : MongoCluster(wrapped), Closeable {

    /**
     * A factory for [MongoClient] instances.
     *
     * @see MongoClient
     * @since 4.10
     */
    public companion object Factory {
        /**
         * Create a new client with the given connection string as if by a call to [create].
         *
         * @param connectionString the connection
         * @return the client
         */
        public fun create(connectionString: String): MongoClient = create(ConnectionString(connectionString))

        /**
         * Create a new client with the given connection string.
         *
         * @param connectionString the connection string, defaults to `mongodb://localhost`.
         * @param mongoDriverInformation any driver information to associate with the MongoClient
         * @return the client
         */
        public fun create(
            connectionString: ConnectionString = ConnectionString("mongodb://localhost"),
            @Nullable mongoDriverInformation: MongoDriverInformation? = null
        ): MongoClient {
            return create(
                MongoClientSettings.builder().applyConnectionString(connectionString).build(), mongoDriverInformation)
        }

        /**
         * Create a new client with the given connection string.
         *
         * For each of the settings classed configurable via [MongoClientSettings], the connection string is applied by
         * calling the `applyConnectionString` method on an instance of setting's builder class, building the setting,
         * and adding it to an instance of [com.mongodb.MongoClientSettings.Builder].
         *
         * @param settings the client settings
         * @param mongoDriverInformation any driver information to associate with the MongoClient
         * @return
         */
        public fun create(
            settings: MongoClientSettings,
            @Nullable mongoDriverInformation: MongoDriverInformation? = null
        ): MongoClient {
            val builder =
                if (mongoDriverInformation == null) MongoDriverInformation.builder()
                else MongoDriverInformation.builder(mongoDriverInformation)
            return MongoClient(JMongoClients.create(settings, builder.driverName("kotlin").build()))
        }
    }

    public override fun close(): Unit = wrapped.close()

    /**
     * Gets the current cluster description.
     *
     * This method will not block, meaning that it may return a [ClusterDescription] whose `clusterType` is unknown and
     * whose [com.mongodb.connection.ServerDescription]s are all in the connecting state. If the application requires
     * notifications after the driver has connected to a member of the cluster, it should register a
     * [com.mongodb.event.ClusterListener] via the [com.mongodb.connection.ClusterSettings] in
     * [com.mongodb.MongoClientSettings].
     *
     * @return the current cluster description
     * @see com.mongodb.connection.ClusterSettings.Builder.addClusterListener
     * @see com.mongodb.MongoClientSettings.Builder.applyToClusterSettings
     */
    public fun getClusterDescription(): ClusterDescription = wrapped.clusterDescription
}
