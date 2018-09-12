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

package com.mongodb.client.model.changestream

import org.bson.BsonDocument
import org.bson.BsonDocumentReader
import org.bson.BsonDocumentWriter
import org.bson.BsonInt32
import org.bson.BsonReader
import org.bson.BsonTimestamp
import org.bson.Document
import org.bson.codecs.DecoderContext
import org.bson.codecs.DocumentCodecProvider
import org.bson.codecs.EncoderContext
import org.bson.codecs.ValueCodecProvider
import spock.lang.Specification

import static org.bson.codecs.configuration.CodecRegistries.fromProviders

class ChangeStreamDocumentCodecSpecification extends Specification {

    def 'should round trip ChangeStreamDocument successfully'() {
        given:
        def codecRegistry = fromProviders([new DocumentCodecProvider(), new ValueCodecProvider()])
        def codec = new ChangeStreamDocumentCodec(clazz, codecRegistry)

        when:
        def writer = new BsonDocumentWriter(new BsonDocument())
        codec.encode(writer, changeStreamDocument, EncoderContext.builder().build())

        then:
        BsonDocument.parse(json) == writer.getDocument()

        BsonReader bsonReader = new BsonDocumentReader(writer.getDocument())
        ChangeStreamDocument actual = codec.decode(bsonReader, DecoderContext.builder().build())

        then:
        changeStreamDocument == actual

        where:
        changeStreamDocument << [
                new ChangeStreamDocument<Document>(
                        BsonDocument.parse('{token: true}'),
                        BsonDocument.parse('{db: "databaseName", coll: "collectionName"}'),
                        Document.parse('{key: "value for fullDocument"}'),
                        new BsonDocument('_id', new BsonInt32(1)),
                        new BsonTimestamp(1234, 2),
                        OperationType.INSERT,
                        null
                ),
                new ChangeStreamDocument<Document>(
                        BsonDocument.parse('{token: true}'),
                        BsonDocument.parse('{db: "databaseName", coll: "collectionName"}'),
                        Document.parse('{key: "value for fullDocument"}'),
                        new BsonDocument('_id', new BsonInt32(2)),
                        null,
                        OperationType.UPDATE,
                        new UpdateDescription(['a', 'b'], BsonDocument.parse('{c: 1}'))
                )
        ]
        clazz << [Document, Document]
        json << [
            '''{_id: {token: true}, ns: {db: "databaseName", coll: "collectionName"}, documentKey : {_id : 1},
                fullDocument: {key: "value for fullDocument"}, clusterTime: { "$timestamp" : { "t" : 1234, "i" : 2 } }
                operationType: "insert"}''',
            '''{_id: {token: true}, ns: {db: "databaseName", coll: "collectionName"}, documentKey : {_id : 2},
                fullDocument: {key: "value for fullDocument"},
                operationType: "update", updateDescription: {removedFields: ["a", "b"], updatedFields: {c: 1}}}''',
        ]
    }
}
