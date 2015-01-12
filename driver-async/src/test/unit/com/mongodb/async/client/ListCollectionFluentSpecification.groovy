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

package com.mongodb.async.client

import com.mongodb.client.model.ListCollectionsOptions
import com.mongodb.client.options.OperationOptions
import com.mongodb.operation.AsyncBatchCursor
import com.mongodb.operation.ListCollectionsOperation
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.Document
import org.bson.codecs.BsonValueCodecProvider
import org.bson.codecs.DocumentCodecProvider
import org.bson.codecs.ValueCodecProvider
import org.bson.codecs.configuration.RootCodecRegistry
import spock.lang.Specification

import static com.mongodb.ReadPreference.secondary
import static java.util.concurrent.TimeUnit.MILLISECONDS

class ListCollectionFluentSpecification extends Specification {

    def codecs = [new ValueCodecProvider(),
                  new DocumentCodecProvider(),
                  new BsonValueCodecProvider()]
    def options = OperationOptions.builder()
            .codecRegistry(new RootCodecRegistry(codecs))
            .readPreference(secondary())
            .build()

    def 'should build the expected listCollectionOperation'() {
        given:
        def cursor = Stub(AsyncBatchCursor) {
            next(_) >> {
                it[0].onResult(null, null)
            }
        }
        def executor = new TestOperationExecutor([cursor, cursor]);
        def listCollectionOptions = new ListCollectionsOptions().filter(new Document('filter', 1)).batchSize(100)
                .maxTime(1000, MILLISECONDS)
        def listCollectionFluent = new ListCollectionsFluentImpl<Document>('db', options, executor, listCollectionOptions, Document)

        when: 'default input should be as expected'
        listCollectionFluent.into([])  { result, t -> }

        def operation = executor.getReadOperation() as ListCollectionsOperation<Document>
        def readPreference = executor.getReadPreference()

        then:
        operation.filter == new BsonDocument('filter', new BsonInt32(1))
        operation.batchSize == 100
        operation.getMaxTime(MILLISECONDS) == 1000
        readPreference == secondary()

        when: 'overriding initial options'
        listCollectionFluent.filter(new Document('filter', 2))
                .batchSize(99)
                .maxTime(999, MILLISECONDS)
                .into([])  { result, t -> }

        operation = executor.getReadOperation() as ListCollectionsOperation<Document>

        then: 'should use the overrides'
        operation.filter == new BsonDocument('filter', new BsonInt32(2))
        operation.batchSize == 99
        operation.getMaxTime(MILLISECONDS) == 999
    }

}
