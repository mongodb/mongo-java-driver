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

import com.mongodb.ServerAddress
import org.bson.BsonArray
import org.bson.BsonString
import org.bson.types.ObjectId
import spock.lang.Specification

class ConnectionDescriptionSpecification extends Specification {
    private final serverId = new ObjectId()
    private final id = new ConnectionId(new ServerId(new ClusterId(), new ServerAddress()))
    private final saslSupportedMechanisms = new BsonArray([new BsonString('SCRAM-SHA-256')])
    private final description = new ConnectionDescription(serverId, id, 5, ServerType.STANDALONE, 1, 2, 3,
            ['zlib'], saslSupportedMechanisms)

    def 'should initialize all values'() {
        expect:
        description.getServiceId() == serverId
        description.connectionId == id
        description.maxWireVersion == 5
        description.serverType == ServerType.STANDALONE
        description.maxBatchCount == 1
        description.maxDocumentSize == 2
        description.maxMessageSize == 3
        description.compressors == ['zlib']
        description.saslSupportedMechanisms == saslSupportedMechanisms
    }

    def 'withConnectionId should return a new instance with the given connectionId and preserve the rest'() {
        given:
        def newId = id.withServerValue(123)
        def newDescription = description.withConnectionId(newId)

        expect:
        !newDescription.is(description)
        newDescription.serviceId == serverId
        newDescription.connectionId == newId
        newDescription.maxWireVersion == 5
        newDescription.serverType == ServerType.STANDALONE
        newDescription.maxBatchCount == 1
        newDescription.maxDocumentSize == 2
        newDescription.maxMessageSize == 3
        newDescription.compressors == ['zlib']
        description.saslSupportedMechanisms == saslSupportedMechanisms
    }

    def 'withServerId should return a new instance with the given serverId and preserve the rest'() {
        given:
        def newServerId = new ObjectId()
        def newDescription = description.withServiceId(newServerId)

        expect:
        !newDescription.is(description)
        newDescription.serviceId == newServerId
        newDescription.connectionId == id
        newDescription.maxWireVersion == 5
        newDescription.serverType == ServerType.STANDALONE
        newDescription.maxBatchCount == 1
        newDescription.maxDocumentSize == 2
        newDescription.maxMessageSize == 3
        newDescription.compressors == ['zlib']
        description.saslSupportedMechanisms == saslSupportedMechanisms
    }
}
