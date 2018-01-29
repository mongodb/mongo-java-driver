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

package com.mongodb.operation

import category.Async
import com.mongodb.MongoExecutionTimeoutException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.ReadPreference
import com.mongodb.async.SingleResultCallback
import com.mongodb.binding.AsyncConnectionSource
import com.mongodb.binding.AsyncReadBinding
import com.mongodb.binding.ConnectionSource
import com.mongodb.binding.ReadBinding
import com.mongodb.connection.AsyncConnection
import com.mongodb.connection.Connection
import com.mongodb.connection.ConnectionDescription
import org.bson.BsonDocument
import org.bson.BsonRegularExpression
import org.bson.Document
import org.bson.codecs.Decoder
import org.bson.codecs.DocumentCodec
import org.junit.experimental.categories.Category
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.disableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.enableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.isSharded
import static java.util.concurrent.TimeUnit.MILLISECONDS

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

        where:
        async << [true, false]
    }

    @IgnoreIf({ isSharded() })
    def 'should throw execution timeout exception from execute'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document())
        def operation = new ListDatabasesOperation(codec).maxTime(1000, MILLISECONDS)

        enableMaxTimeFailPoint()

        when:
        operation.execute(getBinding())

        then:
        thrown(MongoExecutionTimeoutException)

        cleanup:
        disableMaxTimeFailPoint()
    }

    @Category(Async)
    @IgnoreIf({ isSharded() })
    def 'should throw execution timeout exception from executeAsync'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document())
        def operation = new ListDatabasesOperation(codec).maxTime(1000, MILLISECONDS)

        enableMaxTimeFailPoint()

        when:
        executeAsync(operation);

        then:
        thrown(MongoExecutionTimeoutException)

        cleanup:
        disableMaxTimeFailPoint()
    }

    def 'should use the ReadBindings readPreference to set slaveOK'() {
        given:
        def connection = Mock(Connection)
        def connectionSource = Stub(ConnectionSource) {
            getConnection() >> connection
        }
        def readBinding = Stub(ReadBinding) {
            getReadConnectionSource() >> connectionSource
            getReadPreference() >> readPreference
        }
        def operation = new ListDatabasesOperation(helper.decoder)

        when:
        operation.execute(readBinding)

        then:
        _ * connection.getDescription() >> helper.connectionDescription
        1 * connection.command(_, _, _, readPreference, _, _) >> helper.commandResult
        1 * connection.release()

        where:
        readPreference << [ReadPreference.primary(), ReadPreference.secondary()]
    }

    def 'should use the AsyncReadBindings readPreference to set slaveOK'() {
        given:
        def connection = Mock(AsyncConnection)
        def connectionSource = Stub(AsyncConnectionSource) {
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
        1 * connection.commandAsync(_, _, _, readPreference, _, _, _) >> { it[6].onResult(helper.commandResult, null) }
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
