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

import com.mongodb.MongoNamespace
import com.mongodb.ReadPreference
import com.mongodb.ServerAddress
import com.mongodb.connection.ClusterConnectionMode
import com.mongodb.connection.ClusterId
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.ConnectionId
import com.mongodb.connection.ServerId
import com.mongodb.diagnostics.logging.Logger
import com.mongodb.internal.IgnorableRequestContext
import com.mongodb.internal.async.SingleResultCallback
import com.mongodb.internal.validator.NoOpFieldNameValidator
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.codecs.BsonDocumentCodec
import spock.lang.Shared
import spock.lang.Specification

import static com.mongodb.ClusterFixture.getServerApi
import static com.mongodb.CustomMatchers.compare
import static com.mongodb.connection.ServerType.SHARD_ROUTER
import static com.mongodb.connection.ServerType.STANDALONE
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback
import static com.mongodb.internal.connection.MessageHelper.LEGACY_HELLO_LOWER

class DefaultServerConnectionSpecification extends Specification {
    def namespace = new MongoNamespace('test', 'test')
    def internalConnection = Mock(InternalConnection)
    def callback = errorHandlingCallback(Mock(SingleResultCallback), Mock(Logger))
    @Shared
    def standaloneConnectionDescription = new ConnectionDescription(new ConnectionId(new ServerId(new ClusterId(), new ServerAddress())),
            3, STANDALONE, 100, 100, 100, [])
    @Shared
    def mongosConnectionDescription = new ConnectionDescription(new ConnectionId(new ServerId(new ClusterId(), new ServerAddress())),
            3, SHARD_ROUTER, 100, 100, 100, [])

    def 'should execute command protocol asynchronously'() {
        given:
        def command = new BsonDocument(LEGACY_HELLO_LOWER, new BsonInt32(1))
        def validator = new NoOpFieldNameValidator()
        def codec = new BsonDocumentCodec()
        def executor = Mock(ProtocolExecutor)
        def connection = new DefaultServerConnection(internalConnection, executor, ClusterConnectionMode.MULTIPLE)

        when:
        connection.commandAsync('test', command, validator, ReadPreference.primary(), codec, NoOpSessionContext.INSTANCE,
                getServerApi(), IgnorableRequestContext.INSTANCE, callback)

        then:
        1 * executor.executeAsync({
            compare(new CommandProtocolImpl('test', command, validator, ReadPreference.primary(), codec, true, null, null,
                    ClusterConnectionMode.MULTIPLE, getServerApi(), IgnorableRequestContext.INSTANCE), it)
        }, internalConnection, NoOpSessionContext.INSTANCE, callback)
    }
}
