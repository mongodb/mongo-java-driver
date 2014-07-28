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
import com.mongodb.OperationFunctionalSpecification
import org.bson.BsonArray
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonString
import org.junit.experimental.categories.Category
import org.mongodb.Document

import static com.mongodb.ClusterFixture.getAsyncBinding
import static com.mongodb.ClusterFixture.getBinding

class DistinctOperationSpecification extends OperationFunctionalSpecification {

    def 'should be able to distinct by name'() {
        given:
        Document pete = new Document('name', 'Pete').append('age', 38)
        Document sam = new Document('name', 'Sam').append('age', 21)
        Document pete2 = new Document('name', 'Pete').append('age', 25)
        getCollectionHelper().insertDocuments(pete, sam, pete2)

        when:
        DistinctOperation op = new DistinctOperation(getNamespace(), 'name', new Find())
        def result = op.execute(getBinding());

        then:
        result.toList().sort() == new BsonArray([new BsonString('Pete'), new BsonString('Sam')])
    }

    @Category(Async)
    def 'should be able to distinct by name asynchronously'() {
        given:
        Document pete = new Document('name', 'Pete').append('age', 38)
        Document sam = new Document('name', 'Sam').append('age', 21)
        Document pete2 = new Document('name', 'Pete').append('age', 25)
        getCollectionHelper().insertDocuments(pete, sam, pete2)

        when:
        DistinctOperation op = new DistinctOperation(getNamespace(), 'name', new Find())
        def result = op.executeAsync(getAsyncBinding()).get()

        then:
        result.sort() == new BsonArray([new BsonString('Pete'), new BsonString('Sam')])
    }

    def 'should be able to distinct by name with find'() {
        given:
        Document pete = new Document('name', 'Pete').append('age', 38)
        Document sam = new Document('name', 'Sam').append('age', 21)
        Document pete2 = new Document('name', 'Pete').append('age', 25)
        getCollectionHelper().insertDocuments(pete, sam, pete2)

        when:
        DistinctOperation op = new DistinctOperation(getNamespace(), 'name', new Find(new BsonDocument('age', new BsonInt32(25))))
        def result = op.execute(getBinding());

        then:
        result == new BsonArray([new BsonString('Pete')])
    }

    @Category(Async)
    def 'should be able to distinct by name with find asynchronously'() {
        given:
        Document pete = new Document('name', 'Pete').append('age', 38)
        Document sam = new Document('name', 'Sam').append('age', 21)
        Document pete2 = new Document('name', 'Pete').append('age', 25)
        getCollectionHelper().insertDocuments(pete, sam, pete2)

        when:
        DistinctOperation op = new DistinctOperation(getNamespace(), 'name', new Find(new BsonDocument('age', new BsonInt32(25))))
        def result = op.executeAsync(getAsyncBinding()).get()

        then:
        result == new BsonArray([new BsonString('Pete')])
    }
}
