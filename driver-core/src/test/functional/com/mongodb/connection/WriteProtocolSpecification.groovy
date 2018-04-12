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

package com.mongodb.connection

import com.mongodb.OperationFunctionalSpecification
import com.mongodb.ReadPreference
import com.mongodb.bulk.InsertRequest
import com.mongodb.connection.netty.NettyStreamFactory
import com.mongodb.internal.connection.NoOpSessionContext
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonString
import org.bson.codecs.BsonDocumentCodec
import spock.lang.Shared

import static com.mongodb.ClusterFixture.getPrimary
import static com.mongodb.ClusterFixture.getSslSettings
import static com.mongodb.connection.ConnectionFixture.getCredentialListWithCache
import static com.mongodb.connection.ProtocolTestHelper.execute

class WriteProtocolSpecification extends OperationFunctionalSpecification {
    @Shared
    InternalStreamConnection connection;

    def setupSpec() {
        connection = new InternalStreamConnectionFactory(new NettyStreamFactory(SocketSettings.builder().build(), getSslSettings()),
                getCredentialListWithCache(), null, null, [], null)
                .create(new ServerId(new ClusterId(), getPrimary()))
        connection.open();
    }

    def cleanupSpec() {
        connection?.close()
    }

    def 'should ignore write errors on unacknowledged inserts'() {
        given:
        def documentOne = new BsonDocument('_id', new BsonInt32(1))

        def protocol = new InsertProtocol(getNamespace(), true, new InsertRequest(documentOne))

        getCollectionHelper().insertDocuments(documentOne)

        when:
        execute(protocol, connection, async)

        then:
        getCollectionHelper().find(new BsonDocumentCodec()) == [documentOne]

        cleanup:
        // force acknowledgement
        new CommandProtocolImpl(getDatabaseName(), new BsonDocument('drop', new BsonString(getCollectionName())),
                NO_OP_FIELD_NAME_VALIDATOR, ReadPreference.primary(), new BsonDocumentCodec())
                .sessionContext(NoOpSessionContext.INSTANCE)
                .execute(connection)

        where:
        async << [false, true]
    }


}
