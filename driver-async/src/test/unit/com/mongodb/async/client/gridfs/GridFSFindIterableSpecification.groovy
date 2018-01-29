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

package com.mongodb.async.client.gridfs

import com.mongodb.Block
import com.mongodb.CursorType
import com.mongodb.Function
import com.mongodb.MongoNamespace
import com.mongodb.ReadConcern
import com.mongodb.async.AsyncBatchCursor
import com.mongodb.async.FutureResultCallback
import com.mongodb.async.SingleResultCallback
import com.mongodb.async.client.FindIterableImpl
import com.mongodb.async.client.MongoClients
import com.mongodb.async.client.TestOperationExecutor
import com.mongodb.client.gridfs.codecs.GridFSFileCodec
import com.mongodb.client.gridfs.model.GridFSFile
import com.mongodb.client.model.Collation
import com.mongodb.operation.FindOperation
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonObjectId
import org.bson.Document
import org.bson.types.ObjectId
import spock.lang.Specification

import static com.mongodb.CustomMatchers.isTheSameAs
import static com.mongodb.ReadPreference.secondary
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static spock.util.matcher.HamcrestSupport.expect

class GridFSFindIterableSpecification extends Specification {
    def codecRegistry = MongoClients.getDefaultCodecRegistry()
    def gridFSFileCodec = new GridFSFileCodec(codecRegistry)
    def readPreference = secondary()
    def readConcern = ReadConcern.DEFAULT
    def namespace = new MongoNamespace('test', 'fs.files')
    def collation = Collation.builder().locale('en').build()

    def 'should build the expected findOperation'() {
        given:
        def batchCursor = Stub(AsyncBatchCursor)
        def executor = new TestOperationExecutor([batchCursor, batchCursor]);
        def underlying = new FindIterableImpl(null, namespace, GridFSFile, GridFSFile, codecRegistry, readPreference, readConcern, executor,
                new Document())
        def findIterable = new GridFSFindIterableImpl(underlying)

        when: 'default input should be as expected'
        findIterable.batchCursor(Stub(SingleResultCallback))
        def operation = executor.getReadOperation() as FindOperation<GridFSFile>
        def readPreference = executor.getReadPreference()

        then:
        expect operation, isTheSameAs(new FindOperation<GridFSFile>(namespace, gridFSFileCodec).filter(new BsonDocument())
                .slaveOk(true))
        readPreference == secondary()

        when: 'overriding initial options'
        findIterable.filter(new Document('filter', 2))
                .sort(new Document('sort', 2))
                .maxTime(999, MILLISECONDS)
                .batchSize(99)
                .limit(99)
                .skip(9)
                .noCursorTimeout(true)
                .collation(collation)
                .batchCursor(Stub(SingleResultCallback))

        operation = executor.getReadOperation() as FindOperation<GridFSFile>

        then: 'should use the overrides'
        expect operation, isTheSameAs(new FindOperation<GridFSFile>(namespace, gridFSFileCodec)
                .filter(new BsonDocument('filter', new BsonInt32(2)))
                .sort(new BsonDocument('sort', new BsonInt32(2)))
                .maxTime(999, MILLISECONDS)
                .batchSize(99)
                .limit(99)
                .skip(9)
                .noCursorTimeout(true)
                .slaveOk(true)
                .collation(collation)
        )
    }

    def 'should handle mixed types'() {
        given:
        def batchCursor = Stub(AsyncBatchCursor)
        def executor = new TestOperationExecutor([batchCursor])
        def findIterable = new FindIterableImpl(null, namespace, GridFSFile, GridFSFile, codecRegistry, readPreference, readConcern,
                executor, new Document('filter', 1))

        when:
        findIterable.filter(new Document('filter', 1))
                .sort(new BsonDocument('sort', new BsonInt32(1)))
                .modifiers(new Document('modifier', 1))
                .into([], Stub(SingleResultCallback))

        def operation = executor.getReadOperation() as FindOperation<GridFSFile>

        then:
        expect operation, isTheSameAs(new FindOperation<GridFSFile>(namespace, gridFSFileCodec)
                .filter(new BsonDocument('filter', new BsonInt32(1)))
                .sort(new BsonDocument('sort', new BsonInt32(1)))
                .modifiers(new BsonDocument('modifier', new BsonInt32(1)))
                .cursorType(CursorType.NonTailable)
                .slaveOk(true)
        )
    }

    def 'should follow the MongoIterable interface as expected'() {
        given:
        def cannedResults = [
                new GridFSFile(new BsonObjectId(new ObjectId()), 'File 1', 123L, 255, new Date(1438679434041),
                        'd41d8cd98f00b204e9800998ecf8427e', null),
                new GridFSFile(new BsonObjectId(new ObjectId()), 'File 2', 999999L, 255, new Date(1438679434050),
                        'd41d8cd98f00b204e9800998ecf8427e', null),
                new GridFSFile(new BsonObjectId(new ObjectId()), 'File 3', 1L, 255, new Date(1438679434090),
                        'd41d8cd98f00b204e9800998ecf8427e', null),
        ]
        def batchCursor = {
            Stub(AsyncBatchCursor) {
                def count = 0
                def getResult = {
                    count++
                    count == 1 ? cannedResults : null
                }
                next(_) >> { it.last().onResult(getResult(), null) }
            }
        }
        def executor = new TestOperationExecutor([batchCursor(), batchCursor(), batchCursor(), batchCursor()])
        def underlying = new FindIterableImpl(null, namespace, GridFSFile, GridFSFile, codecRegistry, readPreference, readConcern, executor,
                new Document())
        def mongoIterable = new GridFSFindIterableImpl(underlying)

        when:
        def futureResult = new FutureResultCallback()
        mongoIterable.first(futureResult)
        def firstResult = futureResult.get()
        def expectedResult = cannedResults[0]

        then:
        firstResult == expectedResult

        when:
        def count = 0
        mongoIterable.forEach(new Block<GridFSFile>() {
            @Override
            void apply(GridFSFile document) {
                count++
            }
        }, Stub(SingleResultCallback))

        then:
        count == 3

        when:
        def target = []
        mongoIterable.into(target, Stub(SingleResultCallback))

        then:
        target == cannedResults

        when:
        target = []
        mongoIterable.map(new Function<GridFSFile, String>() {
            @Override
            String apply(GridFSFile file) {
                file.getFilename()
            }
        }).into(target, Stub(SingleResultCallback))

        then:
        target == cannedResults*.getFilename()
    }

}
