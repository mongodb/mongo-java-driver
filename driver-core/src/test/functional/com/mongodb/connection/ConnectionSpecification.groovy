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
import com.mongodb.operation.CommandReadOperation
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.codecs.BsonDocumentCodec

import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.connection.ConnectionDescription.getDefaultMaxMessageSize
import static com.mongodb.connection.ConnectionDescription.getDefaultMaxWriteBatchSize

class ConnectionSpecification extends OperationFunctionalSpecification {

    def 'should have id'() {
        when:
        def source = getBinding().getReadConnectionSource()
        def connection = source.connection

        then:
        connection.getDescription().getConnectionId() != null

        cleanup:
        connection?.release()
        source?.release()
    }

    def 'should have description'() {
        when:
        def commandResult = getIsMasterResult()
        def expectedMaxMessageSize = commandResult.getNumber('maxMessageSizeBytes',
                                                             new BsonInt32(getDefaultMaxMessageSize())).intValue()
        def expectedMaxBatchCount = commandResult.getNumber('maxWriteBatchSize',
                                                            new BsonInt32(getDefaultMaxWriteBatchSize())).intValue()
        def source = getBinding().getReadConnectionSource()
        def connection = source.connection

        then:
        connection.description.serverAddress == source.getServerDescription().getAddress()
        connection.description.serverType == source.getServerDescription().getType()
        connection.description.maxDocumentSize == source.getServerDescription().getMaxDocumentSize()
        connection.description.maxMessageSize == expectedMaxMessageSize
        connection.description.maxBatchCount == expectedMaxBatchCount

        cleanup:
        connection?.release()
        source?.release()
    }
   private static BsonDocument getIsMasterResult() {
        new CommandReadOperation<BsonDocument>('admin', new BsonDocument('ismaster', new BsonInt32(1)),
                                               new BsonDocumentCodec()).execute(getBinding())
    }
}
