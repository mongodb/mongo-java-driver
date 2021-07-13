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

package com.mongodb.internal.operation

import com.mongodb.MongoNamespace
import com.mongodb.ReadPreference
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.BsonDocumentCodec
import org.bson.codecs.DocumentCodec

import static com.mongodb.CursorType.TailableAwait
import static java.util.concurrent.TimeUnit.MILLISECONDS

class FindOperationUnitSpecification extends OperationUnitSpecification {

    def 'should find with correct command'() {
        when:
        def operation = new FindOperation<BsonDocument>(namespace, new BsonDocumentCodec())
        def expectedCommand = new BsonDocument('find', new BsonString(namespace.getCollectionName()))

        then:
        testOperation(operation, [3, 2, 0], expectedCommand, async, commandResult)
        // Overrides
        when:
        operation.filter(new BsonDocument('a', BsonBoolean.TRUE))
                .projection(new BsonDocument('x', new BsonInt32(1)))
                .skip(2)
                .limit(limit)
                .batchSize(batchSize)
                .cursorType(TailableAwait)
                .oplogReplay(true)
                .noCursorTimeout(true)
                .partial(true)
                .oplogReplay(true)
                .maxTime(10, MILLISECONDS)
                .comment(new BsonString('my comment'))
                .hint(BsonDocument.parse('{ hint : 1}'))
                .min(BsonDocument.parse('{ abc: 99 }'))
                .max(BsonDocument.parse('{ abc: 1000 }'))
                .returnKey(true)
                .showRecordId(true)

        if (allowDiskUse != null) {
            operation.allowDiskUse(allowDiskUse)
        }

        expectedCommand.append('filter', operation.getFilter())
                .append('projection', operation.getProjection())
                .append('skip', new BsonInt32(operation.getSkip()))
                .append('tailable', BsonBoolean.TRUE)
                .append('awaitData', BsonBoolean.TRUE)
                .append('allowPartialResults', BsonBoolean.TRUE)
                .append('noCursorTimeout', BsonBoolean.TRUE)
                .append('oplogReplay', BsonBoolean.TRUE)
                .append('maxTimeMS', new BsonInt64(operation.getMaxTime(MILLISECONDS)))
                .append('comment', operation.getComment())
                .append('hint', operation.getHint())
                .append('min', operation.getMin())
                .append('max', operation.getMax())
                .append('returnKey', BsonBoolean.TRUE)
                .append('showRecordId', BsonBoolean.TRUE)

        if (allowDiskUse != null) {
            expectedCommand.append('allowDiskUse', new BsonBoolean(allowDiskUse))
        }
        if (commandLimit != null) {
            expectedCommand.append('limit', new BsonInt32(commandLimit))
        }
        if (commandBatchSize != null) {
            expectedCommand.append('batchSize', new BsonInt32(commandBatchSize))
        }
        if (commandSingleBatch != null) {
            expectedCommand.append('singleBatch', BsonBoolean.valueOf(commandSingleBatch))
        }

        then:
        testOperation(operation, version, expectedCommand, async, commandResult)

        where:
        async << [true] * 5 + [false] * 5 + [true] * 5 + [false] * 5
        limit << [100, -100, 100, 0, 100] * 4
        batchSize << [10, 10, -10, 10, 0] * 4
        commandLimit << [100, 100, 10, null, 100] * 4
        commandBatchSize << [10, null, null, 10, null] * 4
        commandSingleBatch << [null, true, true, null, null] * 4
        allowDiskUse << [null] * 10 + [true] * 10
        version << [[3, 2, 0]] * 10 + [[3, 4, 0]] * 10
    }

    def 'should use the readPreference to set secondaryOk for commands'() {
        when:
        def operation = new FindOperation<Document>(namespace, new DocumentCodec())

        then:
        testOperationSecondaryOk(operation, [3, 2, 0], readPreference, async, commandResult)

        where:
        [async, readPreference] << [[true, false], [ReadPreference.primary(), ReadPreference.secondary()]].combinations()
    }

    def 'should throw an exception when using an unsupported Collation'() {
        given:
        def operation = new FindOperation<Document>(getNamespace(), new DocumentCodec())
                .filter(BsonDocument.parse('{str: "FOO"}'))
                .collation(defaultCollation)

        when:
        testOperationThrows(operation, [3, 2, 0], async)

        then:
        def exception = thrown(IllegalArgumentException)
        exception.getMessage().startsWith('Collation not supported by wire version:')

        where:
        async << [true, false]
    }

    def namespace = new MongoNamespace('db', 'coll')
    def decoder = new BsonDocumentCodec()
    def readPreference = ReadPreference.secondary()
    def commandResult = new BsonDocument('cursor', new BsonDocument('id', new BsonInt64(0))
            .append('ns', new BsonString('db.coll'))
            .append('firstBatch', new BsonArrayWrapper([])))
}
