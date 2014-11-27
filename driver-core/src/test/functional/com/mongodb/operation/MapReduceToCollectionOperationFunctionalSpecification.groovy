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
import com.mongodb.MongoNamespace
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.client.test.CollectionHelper
import org.bson.BsonJavaScript
import org.bson.Document
import org.bson.codecs.DocumentCodec
import org.junit.experimental.categories.Category

import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.getBinding

class MapReduceToCollectionOperationFunctionalSpecification extends OperationFunctionalSpecification {
    def mapReduceInputNamespace = new MongoNamespace(getDatabaseName(), 'mapReduceInput')
    def mapReduceOutputNamespace = new MongoNamespace(getDatabaseName(), 'mapReduceOutput')
    def mapReduceOperation = new MapReduceToCollectionOperation(mapReduceInputNamespace,
                                                                new BsonJavaScript('function(){ emit( this.name , 1 ); }'),
                                                                new BsonJavaScript('function(key, values){ return values.length; }'),
                                                                mapReduceOutputNamespace.getCollectionName())
    def expectedResults = [['_id': 'Pete', 'value': 2.0] as Document,
                           ['_id': 'Sam', 'value': 1.0] as Document]
    def helper = new CollectionHelper<Document>(new DocumentCodec(), mapReduceOutputNamespace)

    def setup() {
        CollectionHelper<Document> helper = new CollectionHelper<Document>(new DocumentCodec(), mapReduceInputNamespace)
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')
        Document pete2 = new Document('name', 'Pete').append('job', 'electrician')
        helper.insertDocuments(new DocumentCodec(), pete, sam, pete2)
    }

    def cleanup() {
        new DropCollectionOperation(mapReduceInputNamespace).execute(getBinding())
        new DropCollectionOperation(mapReduceOutputNamespace).execute(getBinding())
    }

    def 'should return the correct statistics and save the results'() {
        when:
        MapReduceStatistics results = mapReduceOperation.execute(getBinding())

        then:
        results.emitCount == 3
        results.inputCount == 3
        results.outputCount == 2
        helper.count() == 2
        helper.find() == expectedResults
    }

    @Category(Async)
    def 'should return the correct statistics and save the results asynchronously'() {

        when:
        MapReduceStatistics results = executeAsync(mapReduceOperation)

        then:
        results.emitCount == 3
        results.inputCount == 3
        results.outputCount == 2
        helper.count() == 2
        helper.find() == expectedResults
    }

}
