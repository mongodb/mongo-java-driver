/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import com.mongodb.MongoNamespace
import com.mongodb.WriteConcern
import com.mongodb.bulk.DeleteRequest
import com.mongodb.bulk.InsertRequest
import com.mongodb.bulk.UpdateRequest
import org.bson.BsonDocument
import org.bson.BsonInt32
import spock.lang.Specification

import static com.mongodb.bulk.WriteRequest.Type.REPLACE

class WriteCommandLimitsSpecification extends Specification {
    def 'should split an insert command when the number of items exceeds the maximum'() {
        given:
        def inserts = []
        (1..4).each {
            inserts.add(new InsertRequest(new BsonDocument()))
        }

        def buffer = new ByteBufferBsonOutput(new SimpleBufferProvider());
        def message = new InsertCommandMessage(new MongoNamespace('test', 'test'), true, WriteConcern.ACKNOWLEDGED, null,
                MessageSettings.builder().maxBatchCount(3)
                        .serverVersion(new ServerVersion(3, 4))
                        .build(),
                inserts);

        when:
        message.encode(buffer)
        def nextMessage = (InsertCommandMessage) message.getEncodingMetadata().nextMessage

        then:
        nextMessage != null
        nextMessage.requests == inserts.subList(3, 4)
    }

    def 'should split an insert command when the number of bytes exceeds the maximum'() {
        given:
        def inserts = []
        (1..4).each {
            inserts.add(new InsertRequest(new BsonDocument('_id', new BsonInt32(it))))
        }

        def buffer = new ByteBufferBsonOutput(new SimpleBufferProvider());
        def message = new InsertCommandMessage(new MongoNamespace('test', 'test'), true, WriteConcern.ACKNOWLEDGED, null,
                MessageSettings.builder().maxDocumentSize(113)
                        .serverVersion(new ServerVersion(3, 4))
                        .build(),
                inserts);

        when:
        message.encode(buffer)
        def nextMessage = (InsertCommandMessage) message.getEncodingMetadata().nextMessage

        then:
        nextMessage != null
        nextMessage.requests == inserts.subList(3, 4)
    }

    def 'should split a delete command when the number of items exceeds the maximum'() {
        given:
        def deletes = []
        (1..4).each {
            deletes.add(new DeleteRequest(new BsonDocument()))
        }

        def buffer = new ByteBufferBsonOutput(new SimpleBufferProvider());
        def message = new DeleteCommandMessage(new MongoNamespace('test', 'test'), true, WriteConcern.ACKNOWLEDGED,
                MessageSettings.builder().maxBatchCount(3)
                        .serverVersion(new ServerVersion(3, 4))
                        .build(),
                deletes);

        when:
        message.encode(buffer)
        def nextMessage = (DeleteCommandMessage) message.getEncodingMetadata().nextMessage

        then:
        nextMessage != null
        nextMessage.requests == deletes.subList(3, 4)
    }

    def 'should split a delete command when the number of bytes exceeds the maximum'() {
        given:
        def deletes = []
        (1..4).each {
            deletes.add(new DeleteRequest(new BsonDocument('_id', new BsonInt32(it))))
        }

        def buffer = new ByteBufferBsonOutput(new SimpleBufferProvider());
        def message = new DeleteCommandMessage(new MongoNamespace('test', 'test'), true, WriteConcern.ACKNOWLEDGED,
                MessageSettings.builder().maxDocumentSize(187)
                        .serverVersion(new ServerVersion(3, 4))
                        .build(),
                deletes);

        when:
        message.encode(buffer)
        def nextMessage = (DeleteCommandMessage) message.getEncodingMetadata().nextMessage

        then:
        nextMessage != null
        nextMessage.requests == deletes.subList(3, 4)
    }

    def 'should split an update command when the number of items exceeds the maximum'() {
        given:
        def replaces = []
        (1..4).each {
            replaces.add(new UpdateRequest(new BsonDocument('_id', new BsonInt32(it)), new BsonDocument(), REPLACE))
        }

        def buffer = new ByteBufferBsonOutput(new SimpleBufferProvider());
        def message = new UpdateCommandMessage(new MongoNamespace('test', 'test'), true, WriteConcern.ACKNOWLEDGED, null,
                MessageSettings.builder().maxBatchCount(3)
                        .serverVersion(new ServerVersion(3, 4))
                        .build(),
                replaces);

        when:
        message.encode(buffer)
        def nextMessage = (UpdateCommandMessage) message.getEncodingMetadata().nextMessage

        then:
        nextMessage != null
        nextMessage.requests == replaces.subList(3, 4)
    }

    def 'should split a replace command when the number of bytes exceeds the maximum'() {
        given:
        def replaces = []
        (1..4).each {
            replaces.add(new UpdateRequest(new BsonDocument('_id', new BsonInt32(it)), new BsonDocument(), REPLACE))
        }

        def buffer = new ByteBufferBsonOutput(new SimpleBufferProvider());
        def message = new UpdateCommandMessage(new MongoNamespace('test', 'test'), true, WriteConcern.ACKNOWLEDGED, null,
                MessageSettings.builder().maxDocumentSize(175)
                        .serverVersion(new ServerVersion(3, 4))
                        .build(),
                replaces);

        when:
        message.encode(buffer)
        def nextMessage = (UpdateCommandMessage) message.getEncodingMetadata().nextMessage

        then:
        nextMessage != null
        nextMessage.requests == replaces.subList(3, 4)
    }
}
