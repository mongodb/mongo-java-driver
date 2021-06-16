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

package com.mongodb.internal.connection

import com.mongodb.MongoSocketOpenException
import com.mongodb.MongoSocketReadTimeoutException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.ServerAddress
import com.mongodb.connection.AsynchronousSocketChannelStreamFactory
import com.mongodb.connection.ClusterId
import com.mongodb.connection.ServerId
import com.mongodb.connection.SocketSettings
import com.mongodb.connection.netty.NettyStreamFactory
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonString
import spock.lang.IgnoreIf
import util.spock.annotations.Slow

import java.util.concurrent.TimeUnit

import static com.mongodb.ClusterFixture.getCredentialWithCache
import static com.mongodb.ClusterFixture.getPrimary
import static com.mongodb.ClusterFixture.getServerApi
import static com.mongodb.ClusterFixture.getSslSettings
import static com.mongodb.internal.connection.CommandHelper.executeCommand

@IgnoreIf({ System.getProperty('ignoreSlowUnitTests') == 'true' })
@Slow
class AsyncStreamTimeoutsSpecification extends OperationFunctionalSpecification {

    static SocketSettings openSocketSettings = SocketSettings.builder().connectTimeout(1, TimeUnit.MILLISECONDS).build()
    static SocketSettings readSocketSettings = SocketSettings.builder().readTimeout(5, TimeUnit.SECONDS).build()

    @IgnoreIf({ getSslSettings().isEnabled() })
    def 'should throw a MongoSocketOpenException when the AsynchronousSocket Stream fails to open'() {
        given:
        def connection = new InternalStreamConnectionFactory(
                new AsynchronousSocketChannelStreamFactory(openSocketSettings, getSslSettings()), getCredentialWithCache(), null, null,
                [], getServerApi())
                .create(new ServerId(new ClusterId(), new ServerAddress(new InetSocketAddress('192.168.255.255', 27017))))

        when:
        connection.open()

        then:
        thrown(MongoSocketOpenException)
    }

    @IgnoreIf({ getSslSettings().isEnabled() })
    def 'should throw a MongoSocketReadTimeoutException with the AsynchronousSocket stream'() {
        given:
        def connection = new InternalStreamConnectionFactory(
                new AsynchronousSocketChannelStreamFactory(readSocketSettings, getSslSettings()), getCredentialWithCache(), null, null,
                [], getServerApi(),).create(new ServerId(new ClusterId(), getPrimary()))
        connection.open()

        getCollectionHelper().insertDocuments(new BsonDocument('_id', new BsonInt32(1)));
        def countCommand = new BsonDocument('count', new BsonString(getCollectionName()))
        countCommand.put('query', new BsonDocument('$where', new BsonString('sleep(5050); return true;')))

        when:
        executeCommand(getDatabaseName(), countCommand, connection)

        then:
        thrown(MongoSocketReadTimeoutException)

        cleanup:
        connection?.close()
    }

    def 'should throw a MongoSocketOpenException when the Netty Stream fails to open'() {
        given:
        def connection = new InternalStreamConnectionFactory(
                new NettyStreamFactory(openSocketSettings, getSslSettings()), getCredentialWithCache(), null, null, [], getServerApi(),
        ).create(new ServerId(new ClusterId(), new ServerAddress(new InetSocketAddress('192.168.255.255', 27017))))

        when:
        connection.open()

        then:
        thrown(MongoSocketOpenException)
    }


    def 'should throw a MongoSocketReadTimeoutException with the Netty stream'() {
        given:
        def connection = new InternalStreamConnectionFactory(
                new NettyStreamFactory(readSocketSettings, getSslSettings()), getCredentialWithCache(), null, null, [], getServerApi(),
        ).create(new ServerId(new ClusterId(), getPrimary()), connectionGeneration)
        connection.open()

        getCollectionHelper().insertDocuments(new BsonDocument('_id', new BsonInt32(1)));
        def countCommand = new BsonDocument('count', new BsonString(getCollectionName()))
        countCommand.put('query', new BsonDocument('$where', new BsonString('sleep(5050); return true;')))

        when:
        executeCommand(getDatabaseName(), countCommand, connection)

        then:
        thrown(MongoSocketReadTimeoutException)

        cleanup:
        connection?.close()
    }

}
