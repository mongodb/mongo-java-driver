package com.mongodb.connection

import com.mongodb.ServerAddress
import spock.lang.Specification

class ConnectionDescriptionSpecification extends Specification {
    private final id = new ConnectionId(new ServerId(new ClusterId(), new ServerAddress()))
    private final version = new ServerVersion(3, 0)
    private final description = new ConnectionDescription(id, version, ServerType.STANDALONE, 1, 2, 3,
    ['zlib'])

    def 'should initialize all values'() {
        expect:
        description.connectionId == id
        description.serverVersion == version
        description.serverType == ServerType.STANDALONE
        description.maxBatchCount == 1
        description.maxDocumentSize == 2
        description.maxMessageSize == 3
        description.compressors == ['zlib']
    }

    def 'withConnectionId should return a new instance with the given connectionId and preserve the rest'() {
        given:
        def newId = id.withServerValue(123)
        def newDescription = description.withConnectionId(newId)

        expect:
        !newDescription.is(description)
        newDescription.connectionId == newId
        newDescription.serverVersion == version
        newDescription.serverType == ServerType.STANDALONE
        newDescription.maxBatchCount == 1
        newDescription.maxDocumentSize == 2
        newDescription.maxMessageSize == 3
        newDescription.compressors == ['zlib']
    }
}