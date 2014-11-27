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

package com.mongodb.operation

import category.Async
import com.mongodb.Block
import com.mongodb.OperationFunctionalSpecification
import org.bson.BsonArray
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonJavaScript
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.DocumentCodec
import org.junit.experimental.categories.Category

import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.loopCursor

class GroupOperationSpecification extends OperationFunctionalSpecification {

    def 'should be able to group by inferring from the reduce function'() {
        given:
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')
        Document pete2 = new Document('name', 'Pete').append('job', 'electrician')
        getCollectionHelper().insertDocuments(new DocumentCodec(), pete, sam, pete2)

        when:
        def result = new GroupOperation(getNamespace(),
                                        new BsonJavaScript('function ( curr, result ) { if (result.name.indexOf(curr.name) == -1) { ' +
                                                           'result.name.push(curr.name); }}'),
                                        new BsonDocument('name': new BsonArray()), new DocumentCodec())
                .execute(getBinding());

        then:
        result.next()[0].name == ['Pete', 'Sam']
    }

    def 'should be able to group by name'() {
        given:
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')
        Document pete2 = new Document('name', 'Pete').append('job', 'electrician')
        getCollectionHelper().insertDocuments(new DocumentCodec(), pete, sam, pete2)

        when:
        def result = new GroupOperation(getNamespace(),
                                        new BsonJavaScript('function ( curr, result ) {}'),
                                        new BsonDocument(), new DocumentCodec())
                .key(new BsonDocument('name', new BsonInt32(1)))
                .execute(getBinding());

        then:
        List<String> results = result.iterator().next()*.getString('name')
        results.containsAll(['Pete', 'Sam'])
    }

    def 'should be able to group by key function'() {
        given:
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')
        Document pete2 = new Document('name', 'Pete').append('job', 'electrician')
        getCollectionHelper().insertDocuments(new DocumentCodec(), pete, sam, pete2)

        when:
        def result = new GroupOperation(getNamespace(),
                                        new BsonJavaScript('function ( curr, result ) { }'),
                                        new BsonDocument(), new DocumentCodec())
                .keyFunction(new BsonJavaScript('function(doc){ return {name: doc.name}; }'))
                .execute(getBinding());

        then:
        List<String> results = result.iterator().next()*.getString('name')
        results.containsAll(['Pete', 'Sam'])
    }


    def 'should be able to group with filter'() {
        given:
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')
        Document pete2 = new Document('name', 'Pete').append('job', 'electrician')
        getCollectionHelper().insertDocuments(new DocumentCodec(), pete, sam, pete2)

        when:
        def result = new GroupOperation(getNamespace(),
                                        new BsonJavaScript('function ( curr, result ) { }'),
                                        new BsonDocument(), new DocumentCodec())
                .key(new BsonDocument('name', new BsonInt32(1)))
                .filter(new BsonDocument('name': new BsonString('Pete')))
                .execute(getBinding());

        then:
        List<String> results = result.iterator().next()*.getString('name')
        results == ['Pete']
    }

    @Category(Async)
    def 'should be able to group by name asynchronously'() {
        given:
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')
        Document pete2 = new Document('name', 'Pete').append('job', 'electrician')
        getCollectionHelper().insertDocuments(new DocumentCodec(), pete, sam, pete2)

        when:
        def operation = new GroupOperation(getNamespace(),
                                        new BsonJavaScript('function ( curr, result ) {}'),
                                        new BsonDocument(), new DocumentCodec())
                .key(new BsonDocument('name', new BsonInt32(1)))

        List<Document> docList = []
        loopCursor(operation, new Block<Document>() {
            @Override
            void apply(final Document value) {
                if (value != null) {
                    docList += value
                }
            }
        });

        then:
        docList.iterator()*.getString('name') containsAll(['Pete', 'Sam'])
    }
}
