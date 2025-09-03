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

package com.mongodb.internal.operation


import com.mongodb.OperationFunctionalSpecification
import com.mongodb.ReadPreference
import com.mongodb.connection.ConnectionDescription
import com.mongodb.internal.async.SingleResultCallback
import com.mongodb.internal.binding.AsyncConnectionSource
import com.mongodb.internal.binding.AsyncReadBinding
import com.mongodb.internal.binding.ConnectionSource
import com.mongodb.internal.binding.ReadBinding
import com.mongodb.internal.connection.AsyncConnection
import com.mongodb.internal.connection.Connection
import org.bson.BsonDocument
import org.bson.BsonRegularExpression
import org.bson.Document
import org.bson.codecs.Decoder
import org.bson.codecs.DocumentCodec

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT

class ListDatabasesOperationSpecification extends OperationFunctionalSpecification {
    def codec = new DocumentCodec()

    def 'should return a list of database names'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('_id', 1))
        def operation = new ListDatabasesOperation(codec)

        when:
        def names = executeAndCollectBatchCursorResults(operation, async)*.get('name')

        then:
        names.contains(getDatabaseName())

        when:
        operation = operation.nameOnly(true).filter(new BsonDocument('name',  new BsonRegularExpression("^${getDatabaseName()}")))
        names = executeAndCollectBatchCursorResults(operation, async)*.get('name')

        then:
        names.contains(getDatabaseName())

        when:
        operation = operation.authorizedDatabasesOnly(true).nameOnly(true)
                .filter(new BsonDocument('name',  new BsonRegularExpression("^${getDatabaseName()}")))
        names = executeAndCollectBatchCursorResults(operation, async)*.get('name')

        then:
        names.contains(getDatabaseName())

        where:
        async << [true, false]
    }

    def 'should use the readPreference to set secondaryOk'() {
        given:
        def connection = Mock(Connection)
        def connectionSource = Stub(ConnectionSource) {
            getConnection() >> connection
            getReadPreference() >> readPreference
            getOperationContext() >> OPERATION_CONTEXT
        }
        def readBinding = Stub(ReadBinding) {
            getReadConnectionSource() >> connectionSource
            getReadPreference() >> readPreference
            getOperationContext() >> OPERATION_CONTEXT
        }
        def operation = new ListDatabasesOperation(helper.decoder)

        when:
        operation.execute(readBinding)

        then:
        _ * connection.getDescription() >> helper.connectionDescription
        1 * connection.command(_, _, _, readPreference, _, OPERATION_CONTEXT) >> helper.commandResult
        1 * connection.release()

        where:
        readPreference << [ReadPreference.primary(), ReadPreference.secondary()]
    }

    def 'should use the readPreference to set secondaryOk async'() {
        given:
        def connection = Mock(AsyncConnection)
        def connectionSource = Stub(AsyncConnectionSource) {
            getReadPreference() >> readPreference
            getConnection(_) >> { it[0].onResult(connection, null) }
        }
        def readBinding = Stub(AsyncReadBinding) {
            getReadPreference() >> readPreference
            getReadConnectionSource(_) >> { it[0].onResult(connectionSource, null) }
        }
        def operation = new ListDatabasesOperation(helper.decoder)

        when:
        operation.executeAsync(readBinding, Stub(SingleResultCallback))

        then:
        _ * connection.getDescription() >> helper.connectionDescription
        1 * connection.commandAsync(_, _, _, readPreference, *_) >> { it.last().onResult(helper.commandResult, null) }
        1 * connection.release()

        where:
        readPreference << [ReadPreference.primary(), ReadPreference.secondary()]
    }

    def helper = [
        decoder: Stub(Decoder),
        commandResult: BsonDocument.parse('{ok: 1.0}').append('databases', new BsonArrayWrapper([])),
        connectionDescription: Stub(ConnectionDescription)
    ]

}
