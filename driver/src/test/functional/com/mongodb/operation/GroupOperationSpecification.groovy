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
import com.mongodb.codecs.DocumentCodec
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonJavaScript
import org.junit.experimental.categories.Category
import org.mongodb.Document

import static com.mongodb.ClusterFixture.getAsyncBinding
import static com.mongodb.ClusterFixture.getBinding

class GroupOperationSpecification extends OperationFunctionalSpecification {

    def 'should be able to group by name'() {
        given:
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')
        Document pete2 = new Document('name', 'Pete').append('job', 'electrician')
        getCollectionHelper().insertDocuments(pete, sam, pete2)

        Group group = new Group(new BsonDocument('name', new BsonInt32(1)), new BsonJavaScript('function ( curr, result ) {}'),
                                new BsonDocument())

        when:
        GroupOperation op = new GroupOperation(getNamespace(), group, new DocumentCodec())
        def result = op.execute(getBinding());

        then:
        List<String> results = result.iterator()*.getString('name')
        results.containsAll(['Pete', 'Sam'])
    }

    @Category(Async)
    def 'should be able to group by name asynchronously'() {
        given:
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')
        Document pete2 = new Document('name', 'Pete').append('job', 'electrician')
        getCollectionHelper().insertDocuments(pete, sam, pete2)

        Group group = new Group(new BsonDocument('name', new BsonInt32(1)), new BsonJavaScript('function ( curr, result ) {}'),
                                new BsonDocument())

        when:
        GroupOperation op = new GroupOperation(getNamespace(), group, new DocumentCodec())
        List<Document> docList = []
        op.executeAsync(getAsyncBinding()).get().forEach(new Block<Document>() {
            @Override
            void apply(final Document value) {
                if (value != null) {
                    docList += value
                }
            }
        }).get()

        then:
        docList.iterator()*.getString('name') containsAll(['Pete', 'Sam'])
    }
}
