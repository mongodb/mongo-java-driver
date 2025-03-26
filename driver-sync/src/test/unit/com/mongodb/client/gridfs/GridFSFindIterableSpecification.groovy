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

package com.mongodb.client.gridfs


import com.mongodb.CursorType
import com.mongodb.Function
import com.mongodb.MongoClientSettings
import com.mongodb.MongoNamespace
import com.mongodb.ReadConcern
import com.mongodb.client.gridfs.codecs.GridFSFileCodec
import com.mongodb.client.gridfs.model.GridFSFile
import com.mongodb.client.internal.FindIterableImpl
import com.mongodb.client.internal.TestOperationExecutor
import com.mongodb.client.model.Collation
import com.mongodb.internal.operation.BatchCursor
import com.mongodb.internal.operation.FindOperation
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonObjectId
import org.bson.Document
import org.bson.types.ObjectId
import spock.lang.Specification

import java.util.function.Consumer

import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS
import static com.mongodb.CustomMatchers.isTheSameAs
import static com.mongodb.ReadPreference.secondary
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static spock.util.matcher.HamcrestSupport.expect

class GridFSFindIterableSpecification extends Specification {

    def codecRegistry = MongoClientSettings.getDefaultCodecRegistry()
    def gridFSFileCodec = new GridFSFileCodec(codecRegistry)
    def readPreference = secondary()
    def readConcern = ReadConcern.DEFAULT
    def collation = Collation.builder().locale('en').build()
    def namespace = new MongoNamespace('test', 'fs.files')

    def 'should build the expected findOperation'() {
        given:
        def executor = new TestOperationExecutor([null, null])
        def underlying = new FindIterableImpl(null, namespace, GridFSFile, GridFSFile, codecRegistry, readPreference, readConcern, executor,
                new Document(), true, TIMEOUT_SETTINGS)
        def findIterable = new GridFSFindIterableImpl(underlying)

        when: 'default input should be as expected'
        findIterable.iterator()

        def operation = executor.getReadOperation() as FindOperation<GridFSFile>
        def readPreference = executor.getReadPreference()

        then:
        expect operation, isTheSameAs(new FindOperation<GridFSFile>(namespace, gridFSFileCodec)
                .filter(new BsonDocument()).retryReads(true))
        readPreference == secondary()

        when: 'overriding initial options'
        findIterable.filter(new Document('filter', 2))
                .sort(new Document('sort', 2))
                .maxTime(100, MILLISECONDS)
                .batchSize(99)
                .limit(99)
                .skip(9)
                .noCursorTimeout(true)
                .collation(collation)
                .iterator()

        operation = executor.getReadOperation() as FindOperation<GridFSFile>

        then: 'should use the overrides'
        expect operation, isTheSameAs(new FindOperation<GridFSFile>(namespace, gridFSFileCodec)
                .filter(new BsonDocument('filter', new BsonInt32(2)))
                .sort(new BsonDocument('sort', new BsonInt32(2)))
                .batchSize(99)
                .limit(99)
                .skip(9)
                .noCursorTimeout(true)
                .collation(collation)
                .retryReads(true)
        )
    }

    def 'should handle mixed types'() {
        given:
        def executor = new TestOperationExecutor([null, null])
        def findIterable = new FindIterableImpl(null, namespace, GridFSFile, GridFSFile, codecRegistry, readPreference, readConcern,
                executor, new Document('filter', 1), true, TIMEOUT_SETTINGS)

        when:
        findIterable.filter(new Document('filter', 1))
                .sort(new BsonDocument('sort', new BsonInt32(1)))
                .iterator()

        def operation = executor.getReadOperation() as FindOperation<GridFSFile>

        then:
        expect operation, isTheSameAs(new FindOperation<GridFSFile>(namespace, gridFSFileCodec)
                .filter(new BsonDocument('filter', new BsonInt32(1)))
                .sort(new BsonDocument('sort', new BsonInt32(1)))
                .cursorType(CursorType.NonTailable)
                .retryReads(true)
        )
    }

    def 'should follow the MongoIterable interface as expected'() {
        given:
        def cannedResults = [
                new GridFSFile(new BsonObjectId(new ObjectId()), 'File 1', 123L, 255, new Date(1438679434041)
                        , null),
                new GridFSFile(new BsonObjectId(new ObjectId()), 'File 2', 999999L, 255, new Date(1438679434050)
                        , null),
                new GridFSFile(new BsonObjectId(new ObjectId()), 'File 3', 1L, 255, new Date(1438679434090)
                        , null),
        ]
        def cursor = {
            Stub(BatchCursor) {
                def count = 0
                def results
                def getResult = {
                    count++
                    results = count == 1 ? cannedResults : null
                    results
                }
                next() >> {
                    getResult()
                }
                hasNext() >> {
                    count == 0
                }
            }
        }
        def executor = new TestOperationExecutor([cursor(), cursor(), cursor(), cursor()])
        def underlying = new FindIterableImpl(null, namespace, GridFSFile, GridFSFile, codecRegistry, readPreference, readConcern, executor,
                new Document(), true, TIMEOUT_SETTINGS)
        def mongoIterable = new GridFSFindIterableImpl(underlying)

        when:
        def firstResult = mongoIterable.first()
        def expectedResult = cannedResults[0]

        then:
        firstResult == expectedResult

        when:
        def count = 0
        mongoIterable.forEach(new Consumer<GridFSFile>() {
            @Override
            void accept(GridFSFile document) {
                count++
            }
        })

        then:
        count == 3

        when:
        def target = []
        mongoIterable.into(target)

        then:
        target == cannedResults

        when:
        target = []
        mongoIterable.map(new Function<GridFSFile, String>() {
            @Override
            String apply(GridFSFile file) {
                file.getFilename()
            }
        }).into(target)

        then:
        target == cannedResults*.getFilename()
    }

}
