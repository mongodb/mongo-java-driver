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

package com.mongodb.reactivestreams.client

import com.mongodb.MongoClientSettings
import com.mongodb.connection.netty.NettyStreamFactoryFactory
import io.netty.channel.oio.OioEventLoopGroup
import io.netty.channel.socket.oio.OioSocketChannel
import org.bson.Document
import reactor.core.publisher.Mono

import static Fixture.getMongoClientBuilderFromConnectionString
import static com.mongodb.ClusterFixture.TIMEOUT_DURATION

@SuppressWarnings('deprecation')
class NettyStreamFactoryFactorySmokeTestSpecification extends FunctionalSpecification {

    private MongoClient mongoClient

    def 'should allow a custom Event Loop Group and Socket Channel'() {
        given:
        def eventLoopGroup = new OioEventLoopGroup()
        def streamFactoryFactory = NettyStreamFactoryFactory.builder()
                .eventLoopGroup(eventLoopGroup)
                .socketChannelClass(OioSocketChannel).build()
        MongoClientSettings settings = getMongoClientBuilderFromConnectionString()
                .streamFactoryFactory(streamFactoryFactory).build()
        def document = new Document('a', 1)

        when:
        mongoClient = MongoClients.create(settings)
        def collection = mongoClient.getDatabase(databaseName).getCollection(collectionName)


        then:
        Mono.from(collection.insertOne(document)).block(TIMEOUT_DURATION)

        then: 'The count is one'
        Mono.from(collection.countDocuments()).block(TIMEOUT_DURATION) == 1

        cleanup:
        mongoClient?.close()
    }

}
