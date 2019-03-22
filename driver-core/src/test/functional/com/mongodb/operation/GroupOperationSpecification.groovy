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

package com.mongodb.operation

import com.mongodb.MongoNamespace
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.ReadPreference
import org.bson.BsonArray
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonJavaScript
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.DocumentCodec
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.serverVersionAtLeast

@IgnoreIf({ serverVersionAtLeast(4, 1) })
class GroupOperationSpecification extends OperationFunctionalSpecification {

    def reduceFunction = new BsonJavaScript('''
            function ( curr, result ) {
                if (result.name.indexOf(curr.name) == -1) { result.name.push(curr.name); }
            }
    ''')
    def initial() {
        new BsonDocument('name': new BsonArray())
    }
    def documentCodec = new DocumentCodec()

    def 'should have the correct defaults and passed values'() {
        when:
        def initial = initial()
        def operation = new GroupOperation(getNamespace(), reduceFunction, initial, documentCodec)

        then:
        operation.getNamespace() == getNamespace()
        operation.getDecoder() == documentCodec
        operation.getReduceFunction() == reduceFunction
        operation.getInitial() == initial
        operation.getKey() == null
        operation.getKeyFunction() == null
        operation.getFinalizeFunction() == null
        operation.getFilter() == null
        operation.getCollation() == null
    }

    def 'should set optional values correctly'(){
        given:
        def filter = new BsonDocument('filter', new BsonInt32(1))
        def key = new BsonDocument('a', new BsonInt32(1))
        def keyFunction = new BsonJavaScript('function(doc){ return {name: doc.name}; }')
        def finalizeFunction = new BsonJavaScript('function(key, value) { return value }')
        def initial = initial()

        when:
        def operation = new GroupOperation(getNamespace(), reduceFunction, initial, documentCodec)
                .filter(filter)
                .key(key)
                .keyFunction(keyFunction)
                .finalizeFunction(finalizeFunction)
                .collation(defaultCollation)

        then:
        operation.getNamespace() == getNamespace()
        operation.getDecoder() == documentCodec
        operation.getReduceFunction() == reduceFunction
        operation.getInitial() == initial
        operation.getFilter() == filter
        operation.getKey() == key
        operation.getKeyFunction() == keyFunction
        operation.getFinalizeFunction() == finalizeFunction
        operation.getCollation() == defaultCollation
    }

    def 'should be able to group by inferring from the reduce function'() {
        given:
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')
        Document pete2 = new Document('name', 'Pete').append('job', 'electrician')
        getCollectionHelper().insertDocuments(new DocumentCodec(), pete, sam, pete2)
        def operation = new GroupOperation(getNamespace(), reduceFunction, initial(), documentCodec)

        when:
        def results = executeAndCollectBatchCursorResults(operation, async)

        then:
        results.head().name == ['Pete', 'Sam']

        where:
        async << [true, false]
    }

    def 'should be able to group by name'() {
        given:
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')
        Document pete2 = new Document('name', 'Pete').append('job', 'electrician')
        getCollectionHelper().insertDocuments(new DocumentCodec(), pete, sam, pete2)

        def operation = new GroupOperation(getNamespace(), new BsonJavaScript('function ( curr, result ) {}'), new BsonDocument(),
                documentCodec).key(new BsonDocument('name', new BsonInt32(1)))

        when:
        def results = executeAndCollectBatchCursorResults(operation, async)

        then:
        results*.getString('name').containsAll(['Pete', 'Sam'])

        where:
        async << [true, false]
    }

    def 'should be able to group by key function'() {
        given:
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')
        Document pete2 = new Document('name', 'Pete').append('job', 'electrician')
        getCollectionHelper().insertDocuments(new DocumentCodec(), pete, sam, pete2)
        def operation = new GroupOperation(getNamespace(),  new BsonJavaScript('function ( curr, result ) { }'), new BsonDocument(),
                documentCodec).keyFunction(new BsonJavaScript('function(doc){ return {name: doc.name}; }'))

        when:
        def results = executeAndCollectBatchCursorResults(operation, async)

        then:
        results*.getString('name').containsAll(['Pete', 'Sam'])

        where:
        async << [true, false]
    }


    def 'should be able to group with filter'() {
        given:
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')
        Document pete2 = new Document('name', 'Pete').append('job', 'electrician')
        getCollectionHelper().insertDocuments(new DocumentCodec(), pete, sam, pete2)
        def operation = new GroupOperation(getNamespace(),
                                        new BsonJavaScript('function ( curr, result ) { }'),
                                        new BsonDocument(), documentCodec)
                .key(new BsonDocument('name', new BsonInt32(1)))
                .filter(new BsonDocument('name': new BsonString('Pete')))

        when:
        def results = executeAndCollectBatchCursorResults(operation, async)

        then:
        results*.getString('name') == ['Pete']

        where:
        async << [true, false]
    }

    def 'should use the ReadBindings readPreference to set slaveOK'() {
        when:
        def commandResult = BsonDocument.parse('{ok: 1.0}').append('retval', new BsonArrayWrapper([]))
        def namespace = new MongoNamespace('db', 'coll')
        def operation = new GroupOperation(namespace, new BsonJavaScript('function ( curr, result ) { }'), new BsonDocument(),
                documentCodec).key(BsonDocument.parse('{name: 1}'))

        then:
        testOperationSlaveOk(operation, [3, 2, 0], readPreference, async, commandResult)

        where:
        [async, readPreference] << [[true, false], [ReadPreference.primary(), ReadPreference.secondary()]].combinations()
    }

    def 'should throw an exception when using an unsupported Collation'() {
        def operation = new GroupOperation(namespace, new BsonJavaScript('function ( curr, result ) { }'), new BsonDocument(),
                documentCodec).key(BsonDocument.parse('{str: 1}')).collation(caseInsensitiveCollation)

        when:
        testOperationThrows(operation, [3, 2, 0], async)

        then:
        def exception = thrown(IllegalArgumentException)
        exception.getMessage().startsWith('Collation not supported by wire version:')

        where:
        async << [false, false]
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 4) })
    def 'should support collation'() {
        given:
        def document = Document.parse('{str: "foo"}')
        getCollectionHelper().insertDocuments(document)
        def operation = new GroupOperation(namespace, new BsonJavaScript('function ( curr, result ) { }'), new BsonDocument(),
                documentCodec).key(BsonDocument.parse('{str: 1}')).collation(caseInsensitiveCollation)

        when:
        def result = executeAndCollectBatchCursorResults(operation, async)

        then:
        result == [document]

        where:
        async << [true, false]
    }
}
