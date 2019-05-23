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
                new ChangeStreamDocument<Document>(OperationType.INSERT,
                        BsonDocument.parse('{token: true}'),
                        BsonDocument.parse('{db: "engineering", coll: "users"}'),
                        null,
                        Document.parse('{_id: 1, userName: "alice123", name: "Alice"}'),
                        BsonDocument.parse('{userName: "alice123", _id: 1}'),
                        new BsonTimestamp(1234, 2)
                        ,
                        null
                ),
                new ChangeStreamDocument<Document>(OperationType.UPDATE,
                        BsonDocument.parse('{token: true}'),
                        BsonDocument.parse('{db: "engineering", coll: "users"}'),
                        null,
                        null,
                        BsonDocument.parse('{_id: 1}'),
                        new BsonTimestamp(1234, 2)
                        ,
                        new UpdateDescription(['phoneNumber'], BsonDocument.parse('{email: "alice@10gen.com"}'))
                ),
                new ChangeStreamDocument<Document>(OperationType.UPDATE,
                        BsonDocument.parse('{token: true}'),
                        BsonDocument.parse('{db: "engineering", coll: "users"}'),
                        null,
                        Document.parse('{_id: 1, userName: "alice123", name: "Alice"}'),
                        BsonDocument.parse('{_id: 1}'),
                        new BsonTimestamp(1234, 2)
                        ,
                        new UpdateDescription(['phoneNumber'], BsonDocument.parse('{email: "alice@10gen.com"}'))
                ),
                new ChangeStreamDocument<Document>(OperationType.REPLACE,
                        BsonDocument.parse('{token: true}'),
                        BsonDocument.parse('{db: "engineering", coll: "users"}'),
                        null,
                        Document.parse('{_id: 1, userName: "alice123", name: "Alice"}'),
                        BsonDocument.parse('{_id: 1}'),
                        new BsonTimestamp(1234, 2)
                        ,
                        null
                ),
                new ChangeStreamDocument<Document>(OperationType.DELETE,
                        BsonDocument.parse('{token: true}'),
                        BsonDocument.parse('{db: "engineering", coll: "users"}'),
                        null,
                        null,
                        BsonDocument.parse('{_id: 1}'),
                        new BsonTimestamp(1234, 2)
                        ,
                        null
                ),
                new ChangeStreamDocument<Document>(OperationType.DROP,
                        BsonDocument.parse('{token: true}'),
                        BsonDocument.parse('{db: "engineering", coll: "users"}'),
                        null,
                        null,
                        null,
                        new BsonTimestamp(1234, 2)
                        ,
                        null
                ),
                new ChangeStreamDocument<Document>(OperationType.RENAME,
                        BsonDocument.parse('{token: true}'),
                        BsonDocument.parse('{db: "engineering", coll: "users"}'),
                        BsonDocument.parse('{db: "engineering", coll: "people"}'),
                        null,
                        null,
                        new BsonTimestamp(1234, 2)
                        ,
                        null
                ),
                new ChangeStreamDocument<Document>(OperationType.DROP_DATABASE,
                        BsonDocument.parse('{token: true}'),
                        BsonDocument.parse('{db: "engineering"}'),
                        null,
                        null,
                        null,
                        new BsonTimestamp(1234, 2)
                        ,
                        null
                ),
                new ChangeStreamDocument<Document>(OperationType.INVALIDATE,
                        BsonDocument.parse('{token: true}'),
                        null,
                        null,
                        null,
                        null,
                        new BsonTimestamp(1234, 2)
                        ,
                        null
                ),
        ]
        clazz << [Document, Document, Document, Document, Document, Document, Document, Document, Document
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
   },
   fullDocument: {
      _id: 1,
      name: 'Alice',
      userName: 'alice123'
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
'''
        ]
    }
}
