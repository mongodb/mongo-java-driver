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

import org.bson.BsonBoolean
import org.bson.BsonDateTime
import org.bson.BsonDocument
import org.bson.BsonDocumentReader
import org.bson.BsonDocumentWriter
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonReader
import org.bson.BsonTimestamp
import org.bson.Document
import org.bson.codecs.DecoderContext
import org.bson.codecs.DocumentCodecProvider
import org.bson.codecs.EncoderContext
import org.bson.codecs.ValueCodecProvider
import spock.lang.Specification

import static java.util.Collections.singletonList
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

        when:
        BsonReader bsonReader = new BsonDocumentReader(writer.getDocument())
        ChangeStreamDocument actual = codec.decode(bsonReader, DecoderContext.builder().build())

        then:
        changeStreamDocument == actual

        where:
        changeStreamDocument << [
                new ChangeStreamDocument<Document>(OperationType.INSERT.value,
                        BsonDocument.parse('{token: true}'),
                        BsonDocument.parse('{db: "engineering", coll: "users"}'),
                        null,
                        Document.parse('{_id: 1, userName: "alice123", name: "Alice"}'),
                        null,
                        BsonDocument.parse('{userName: "alice123", _id: 1}'),
                        new BsonTimestamp(1234, 2)
                        ,
                        null, null, null, null, null
                ),
                new ChangeStreamDocument<Document>(OperationType.UPDATE.value,
                        BsonDocument.parse('{token: true}'),
                        BsonDocument.parse('{db: "engineering", coll: "users"}'),
                        null,
                        null,
                        null,
                        BsonDocument.parse('{_id: 1}'),
                        new BsonTimestamp(1234, 2)
                        ,
                        new UpdateDescription(['phoneNumber'], BsonDocument.parse('{email: "alice@10gen.com"}'), null),
                        null, null, null, null
                ),
                new ChangeStreamDocument<Document>(OperationType.UPDATE.value,
                        BsonDocument.parse('{token: true}'),
                        BsonDocument.parse('{db: "engineering", coll: "users"}'),
                        null,
                        Document.parse('{_id: 1, userName: "alice123", name: "Alice"}'),
                        Document.parse('{_id: 1, userName: "alice1234", name: "Alice"}'),
                        BsonDocument.parse('{_id: 1}'),
                        new BsonTimestamp(1234, 2)
                        ,
                        new UpdateDescription(['phoneNumber'], BsonDocument.parse('{email: "alice@10gen.com"}'),
                                singletonList(new TruncatedArray('education', 2))),
                        null, null, null, null
                ),
                new ChangeStreamDocument<Document>(OperationType.REPLACE.value,
                        BsonDocument.parse('{token: true}'),
                        BsonDocument.parse('{db: "engineering", coll: "users"}'),
                        null,
                        Document.parse('{_id: 1, userName: "alice123", name: "Alice"}'),
                        Document.parse('{_id: 1, userName: "alice1234", name: "Alice"}'),
                        BsonDocument.parse('{_id: 1}'),
                        new BsonTimestamp(1234, 2)
                        ,
                        null, null, null, null, null
                ),
                new ChangeStreamDocument<Document>(OperationType.DELETE.value,
                        BsonDocument.parse('{token: true}'),
                        BsonDocument.parse('{db: "engineering", coll: "users"}'),
                        null,
                        null,
                        Document.parse('{_id: 1, userName: "alice123", name: "Alice"}'),
                        BsonDocument.parse('{_id: 1}'),
                        new BsonTimestamp(1234, 2)
                        ,
                        null, null, null, null, null
                ),
                new ChangeStreamDocument<Document>(OperationType.DROP.value,
                        BsonDocument.parse('{token: true}'),
                        BsonDocument.parse('{db: "engineering", coll: "users"}'),
                        null,
                        null,
                        null,
                        null,
                        new BsonTimestamp(1234, 2)
                        ,
                        null, null, null, null, null
                ),
                new ChangeStreamDocument<Document>(OperationType.RENAME.value,
                        BsonDocument.parse('{token: true}'),
                        BsonDocument.parse('{db: "engineering", coll: "users"}'),
                        BsonDocument.parse('{db: "engineering", coll: "people"}'),
                        null,
                        null,
                        null,
                        new BsonTimestamp(1234, 2)
                        ,
                        null, null, null, null, null
                ),
                new ChangeStreamDocument<Document>(OperationType.DROP_DATABASE.value,
                        BsonDocument.parse('{token: true}'),
                        BsonDocument.parse('{db: "engineering"}'),
                        null,
                        null,
                        null,
                        null,
                        new BsonTimestamp(1234, 2)
                        ,
                        null, null, null, null, null
                ),
                new ChangeStreamDocument<Document>(OperationType.INVALIDATE.value,
                        BsonDocument.parse('{token: true}'),
                        null,
                        null,
                        null,
                        null,
                        null,
                        new BsonTimestamp(1234, 2)
                        ,
                        null, null, null, null, null
                ),
                new ChangeStreamDocument<Document>(OperationType.INSERT.value,
                        BsonDocument.parse('{token: true}'),
                        BsonDocument.parse('{db: "engineering", coll: "users"}'),
                        null,
                        Document.parse('{_id: 1, userName: "alice123", name: "Alice"}'),
                        null,
                        BsonDocument.parse('{userName: "alice123", _id: 1}'),
                        new BsonTimestamp(1234, 2),
                        null,
                        new BsonInt64(1),
                        BsonDocument.parse('{id: 1, uid: 2}'),
                        new BsonDateTime(42),
                        new BsonDocument('extra', BsonBoolean.TRUE).append('value', new BsonInt32(1))
                ),
        ]
        clazz << [Document, Document, Document, Document, Document, Document, Document, Document, Document, Document
        ]
        json << [
                '''
{
   _id: { token : true },
   operationType: 'insert',
   clusterTime: { "$timestamp" : { "t" : 1234, "i" : 2 } },
   ns: {
      db: 'engineering',
      coll: 'users'
   },
   documentKey: {
      userName: 'alice123',
      _id: 1
   },
   fullDocument: {
      _id: 1,
      userName: 'alice123',
      name: 'Alice'
   }
}
''',
                '''
{
   _id: { token : true },
   operationType: 'update',
   clusterTime: { "$timestamp" : { "t" : 1234, "i" : 2 } },
   ns: {
      db: 'engineering',
      coll: 'users'
   },
   documentKey: {
      _id: 1
   },
   updateDescription: {
      updatedFields: {
         email: 'alice@10gen.com'
      },
      removedFields: ['phoneNumber']
      "truncatedArrays": []
   }
}
''',
                '''
{
   _id: { token : true },
   operationType: 'update',
   clusterTime: { "$timestamp" : { "t" : 1234, "i" : 2 } },
   ns: {
      db: 'engineering',
      coll: 'users'
   },
   documentKey: {
      _id: 1
   },
   updateDescription: {
      updatedFields: {
         email: 'alice@10gen.com'
      },
      removedFields: ['phoneNumber'],
      "truncatedArrays": [
         {
            "field": "education",
            "newSize": 2
         }
      ]
   },
   fullDocument: {
      _id: 1,
      name: 'Alice',
      userName: 'alice123'
   },
   fullDocumentBeforeChange: {
      _id: 1,
      name: 'Alice',
      userName: 'alice1234'
   }
}
''',
                '''
{
   _id: { token : true },
   operationType: 'replace',
   clusterTime: { "$timestamp" : { "t" : 1234, "i" : 2 } },
   ns: {
      db: 'engineering',
      coll: 'users'
   },
   documentKey: {
      _id: 1
   },
   fullDocument: {
      _id: 1,
      userName: 'alice123',
      name: 'Alice'
   },
      fullDocumentBeforeChange: {
      _id: 1,
      name: 'Alice',
      userName: 'alice1234'
   }
}
''',
                '''
{
   _id: { token : true },
   operationType: 'delete',
   clusterTime: { "$timestamp" : { "t" : 1234, "i" : 2 } },
   ns: {
      db: 'engineering',
      coll: 'users'
   },
   documentKey: {
      _id: 1
   },
   fullDocumentBeforeChange: {
      _id: 1,
      name: 'Alice',
      userName: 'alice123'
   }
}
''',
                '''
{
   _id: { token : true },
   operationType: 'drop',
   clusterTime: { "$timestamp" : { "t" : 1234, "i" : 2 } },
   ns: {
      db: 'engineering',
      coll: 'users'
   }
}
''',
                '''
{
   _id: { token : true },
   operationType: 'rename',
   clusterTime: { "$timestamp" : { "t" : 1234, "i" : 2 } },
   ns: {
      db: 'engineering',
      coll: 'users'
   },
   to: {
      db: 'engineering',
      coll: 'people'
   }
}
''',
                '''
{
   _id: { token : true },
   operationType: 'dropDatabase',
   clusterTime: { "$timestamp" : { "t" : 1234, "i" : 2 } },
   ns: {
      db: 'engineering'
   }
}
''',
                '''
{
   _id: { token : true },
   operationType: 'invalidate',
   clusterTime: { "$timestamp" : { "t" : 1234, "i" : 2 } }
}
''',
                '''
{
   _id: { token : true },
   operationType: 'insert',
   clusterTime: { "$timestamp" : { "t" : 1234, "i" : 2 } },
   ns: {
      db: 'engineering',
      coll: 'users'
   },
   documentKey: {
      userName: 'alice123',
      _id: 1
   },
   fullDocument: {
      _id: 1,
      userName: 'alice123',
      name: 'Alice'
   },
   txnNumber: NumberLong('1'),
   lsid: {
      id: 1,
      uid: 2
   },
   wallTime: {$date: 42},
   extra: true,
   value: 1
}
''',
        ]
    }
}
