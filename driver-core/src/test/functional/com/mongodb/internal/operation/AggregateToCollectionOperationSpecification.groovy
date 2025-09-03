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

import com.mongodb.MongoCommandException
import com.mongodb.MongoNamespace
import com.mongodb.MongoWriteConcernException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.ReadConcern
import com.mongodb.ReadPreference
import com.mongodb.WriteConcern
import com.mongodb.client.model.Aggregates
import com.mongodb.client.model.CreateCollectionOptions
import com.mongodb.client.model.Filters
import com.mongodb.client.model.ValidationOptions
import com.mongodb.client.test.CollectionHelper
import com.mongodb.internal.client.model.AggregationLevel
import org.bson.BsonArray
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.BsonDocumentCodec
import org.bson.codecs.BsonValueCodecProvider
import org.bson.codecs.DocumentCodec
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet
import static com.mongodb.ClusterFixture.isSharded
import static com.mongodb.WriteConcern.ACKNOWLEDGED
import static com.mongodb.client.model.Filters.gte
import static org.bson.codecs.configuration.CodecRegistries.fromProviders

class AggregateToCollectionOperationSpecification extends OperationFunctionalSpecification {
    def registry = fromProviders([new BsonValueCodecProvider()])

    def aggregateCollectionNamespace = new MongoNamespace(getDatabaseName(), 'aggregateCollectionName')

    def setup() {
        CollectionHelper.drop(aggregateCollectionNamespace)
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')
        Document pete2 = new Document('name', 'Pete').append('job', 'electrician')
        getCollectionHelper().insertDocuments(new DocumentCodec(), pete, sam, pete2)
    }

    def 'should have the correct defaults'() {
        given:
        def pipeline = [new BsonDocument('$out', new BsonString(aggregateCollectionNamespace.collectionName))]

        when:
        AggregateToCollectionOperation operation = createOperation(getNamespace(), pipeline, ACKNOWLEDGED)

        then:
        operation.getAllowDiskUse() == null
        operation.getPipeline() == pipeline
        operation.getBypassDocumentValidation() == null
        operation.getWriteConcern() == ACKNOWLEDGED
        operation.getCollation() == null
    }

    def 'should set optional values correctly (with write concern)'(){
        given:
        def pipeline = [new BsonDocument('$out', new BsonString(aggregateCollectionNamespace.collectionName))]

        when:
        AggregateToCollectionOperation operation =
                createOperation(getNamespace(), pipeline, WriteConcern.MAJORITY)
                .allowDiskUse(true)
                .bypassDocumentValidation(true)
                .collation(defaultCollation)

        then:
        operation.getAllowDiskUse()
        operation.getBypassDocumentValidation() == true
        operation.getWriteConcern() == WriteConcern.MAJORITY
        operation.getCollation() == defaultCollation
    }

    def 'should set optional values correctly (with read concern)'(){
        given:
        def pipeline = [new BsonDocument('$out', new BsonString(aggregateCollectionNamespace.collectionName))]

        when:
        AggregateToCollectionOperation operation = createOperation(getNamespace(), pipeline, ReadConcern.DEFAULT)
                .allowDiskUse(true)
                .bypassDocumentValidation(true)
                .collation(defaultCollation)

        then:
        operation.getAllowDiskUse()
        operation.getBypassDocumentValidation() == true
        operation.getReadConcern() == ReadConcern.DEFAULT
        operation.getCollation() == defaultCollation
    }

    def 'should not accept an empty pipeline'() {
        when:
        createOperation(getNamespace(), [], ACKNOWLEDGED)


        then:
        thrown(IllegalArgumentException)
    }

    def 'should be able to output to a collection'() {
        when:
        AggregateToCollectionOperation operation = createOperation(getNamespace(),
                [new BsonDocument('$out', new BsonString(aggregateCollectionNamespace.collectionName))],
                ACKNOWLEDGED)
        execute(operation, async)

        then:
        getCollectionHelper(aggregateCollectionNamespace).count() == 3

        where:
        async << [true, false]
    }

    def 'should be able to merge into a collection'() {
        when:
        AggregateToCollectionOperation operation = createOperation(getNamespace(),
                [new BsonDocument('$merge', new BsonDocument('into', new BsonString(aggregateCollectionNamespace.collectionName)))])
        execute(operation, async)

        then:
        getCollectionHelper(aggregateCollectionNamespace).count() == 3

        where:
        async << [true, false]
    }

    def 'should be able to match then output to a collection'() {
        when:
        AggregateToCollectionOperation operation = createOperation(getNamespace(),
                [new BsonDocument('$match', new BsonDocument('job', new BsonString('plumber'))),
                 new BsonDocument('$out', new BsonString(aggregateCollectionNamespace.collectionName))], ACKNOWLEDGED)
        execute(operation, async)

        then:
        getCollectionHelper(aggregateCollectionNamespace).count() == 1

        where:
        async << [true, false]
    }

    @IgnoreIf({ !isDiscoverableReplicaSet() })
    def 'should throw on write concern error'() {
        given:
        AggregateToCollectionOperation operation = createOperation(getNamespace(),
                        [new BsonDocument('$out', new BsonString(aggregateCollectionNamespace.collectionName))],
                        new WriteConcern(5))

        when:
        execute(operation, async)

        then:
        def ex = thrown(MongoWriteConcernException)
        ex.writeConcernError.code == 100
        ex.writeResult.wasAcknowledged()

        where:
        async << [true, false]
    }

    def 'should support bypassDocumentValidation'() {
        given:
        def collectionOutHelper = getCollectionHelper(new MongoNamespace(getDatabaseName(), 'collectionOut'))
        collectionOutHelper.create('collectionOut', new CreateCollectionOptions().validationOptions(
                new ValidationOptions().validator(gte('level', 10))))
        getCollectionHelper().insertDocuments(BsonDocument.parse('{ level: 9 }'))

        when:
        AggregateToCollectionOperation operation = createOperation(getNamespace(),
                [BsonDocument.parse('{$out: "collectionOut"}')], ACKNOWLEDGED)
        execute(operation, async)

        then:
        thrown(MongoCommandException)

        when:
        execute(operation.bypassDocumentValidation(false), async)

        then:
        thrown(MongoCommandException)

        when:
        execute(operation.bypassDocumentValidation(true), async)

        then:
        notThrown(MongoCommandException)

        cleanup:
        collectionOutHelper?.drop()

        where:
        async << [true, false]
    }

    def 'should create the expected command'() {
        when:
        def pipeline = [BsonDocument.parse('{$out: "collectionOut"}')]
        AggregateToCollectionOperation operation = new AggregateToCollectionOperation(getNamespace(), pipeline,
                ReadConcern.MAJORITY, WriteConcern.MAJORITY)
                .bypassDocumentValidation(true)
        def expectedCommand = new BsonDocument('aggregate', new BsonString(getNamespace().getCollectionName()))
                .append('pipeline', new BsonArray(pipeline))

        if (includeBypassValidation) {
            expectedCommand.put('bypassDocumentValidation', BsonBoolean.TRUE)
        }
        if (includeReadConcern) {
            expectedCommand.append('readConcern', new BsonDocument('level', new BsonString('majority')))
        }
        if (includeWriteConcern) {
            expectedCommand.append('writeConcern', new BsonDocument('w', new BsonString('majority')))
        }
        if (includeCollation) {
            operation.collation(defaultCollation)
            expectedCommand.append('collation', defaultCollation.asDocument())
        }
        if (useCursor) {
            expectedCommand.append('cursor', new BsonDocument())
        }
        if (useHint) {
            operation.hint(new BsonString('x_1'))
            expectedCommand.append('hint', new BsonString('x_1'))
        }

        then:
        testOperation(operation, serverVersion, expectedCommand, async, BsonDocument.parse('{ok: 1}'),
                true, false, ReadPreference.primary(), false)

        where:
        serverVersion | includeBypassValidation | includeReadConcern | includeWriteConcern | includeCollation | async  | useCursor | useHint
        [3, 6, 0]     | true                    | true               | true                | true             | true   | true      | true
        [3, 6, 0]     | true                    | true               | true                | true             | false  | true      | false
    }

    def 'should support collation'() {
        given:
        getCollectionHelper().insertDocuments(BsonDocument.parse('{_id: 1, str: "foo"}'))
        def pipeline = [BsonDocument.parse('{$match: {str: "FOO"}}'),
                        new BsonDocument('$out', new BsonString(aggregateCollectionNamespace.collectionName))]
        AggregateToCollectionOperation operation = createOperation(getNamespace(), pipeline, ACKNOWLEDGED)
                .collation(caseInsensitiveCollation)

        when:
        execute(operation, async)

        then:
        getCollectionHelper(aggregateCollectionNamespace).count() == 1

        where:
        async << [true, false]
    }

    @IgnoreIf({ isSharded() })
    def 'should apply comment'() {
        given:
        def profileCollectionHelper = getCollectionHelper(new MongoNamespace(getDatabaseName(), 'system.profile'))
        new CommandReadOperation<>(getDatabaseName(), new BsonDocument('profile', new BsonInt32(2)),
                new BsonDocumentCodec()).execute(getBinding())
        def expectedComment = 'this is a comment'
        AggregateToCollectionOperation operation = createOperation(getNamespace(),
                [Aggregates.out('outputCollection').toBsonDocument(BsonDocument, registry)], ACKNOWLEDGED)
                .comment(new BsonString(expectedComment))

        when:
        execute(operation, async)

        then:
        Document profileDocument = profileCollectionHelper.find(Filters.exists('command.aggregate')).get(0)
        ((Document) profileDocument.get('command')).get('comment') == expectedComment

        cleanup:
        new CommandReadOperation<>(getDatabaseName(), new BsonDocument('profile', new BsonInt32(0)),
                new BsonDocumentCodec()).execute(getBinding())
        profileCollectionHelper.drop()

        where:
        async << [true, false]
    }

    def createOperation(final MongoNamespace namespace, final List<BsonDocument> pipeline) {
        new AggregateToCollectionOperation(namespace, pipeline, null, null, AggregationLevel.COLLECTION)
    }

    def createOperation(final MongoNamespace namespace, final List<BsonDocument> pipeline, final WriteConcern writeConcern) {
        new AggregateToCollectionOperation(namespace, pipeline, null, writeConcern, AggregationLevel.COLLECTION)
    }

    def createOperation(final MongoNamespace namespace, final List<BsonDocument> pipeline, final ReadConcern readConcern) {
        new AggregateToCollectionOperation(namespace, pipeline, readConcern, null, AggregationLevel.COLLECTION)
    }

}
