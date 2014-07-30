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
import com.mongodb.codecs.DocumentCodec
import org.bson.BsonJavaScript
import org.junit.experimental.categories.Category
import org.mongodb.Document
import spock.lang.Shared

import static com.mongodb.ClusterFixture.getAsyncBinding
import static com.mongodb.ClusterFixture.getBinding

class MapReduceToCollectionOperationFunctionalSpecification extends OperationFunctionalSpecification {
    private final documentCodec = new DocumentCodec()
    def mapReduce = new MapReduce(new BsonJavaScript('function(){ emit( this.name , 1 ); }'),
                                  new BsonJavaScript('function(key, values){ return values.length; }'),
                                  new MapReduceOutputOptions('mapReduceOutput'))
    def expectedResults = [['_id': 'Pete', 'value': 2.0] as Document,
                           ['_id': 'Sam', 'value': 1.0] as Document]

    @Shared mapReduceInputNamespace
    @Shared mapReduceOutputNamespace

    def setup() {
        mapReduceInputNamespace = new MongoNamespace(getDatabaseName(), 'mapReduceInput')
        mapReduceOutputNamespace = new MongoNamespace(getDatabaseName(), 'mapReduceOutput')

        CollectionHelper<Document> helper = new CollectionHelper<Document>(documentCodec, mapReduceInputNamespace)
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')
        Document pete2 = new Document('name', 'Pete').append('job', 'electrician')
        helper.insertDocuments(pete, sam, pete2)
    }

    def cleanup() {
        new DropCollectionOperation(mapReduceInputNamespace).execute(getBinding())
        new DropCollectionOperation(mapReduceOutputNamespace).execute(getBinding())
    }

    def 'should return the correct statistics and save the results'() {
        given:
        def helper = new CollectionHelper<Document>(documentCodec, mapReduceOutputNamespace)
        def operation = new MapReduceToCollectionOperation(mapReduceInputNamespace, mapReduce)

        when:
        MapReduceStatistics results = operation.execute(getBinding())
        def serverUsed = operation.getServerUsed()

        then:
        results.emitCount == 3
        results.inputCount == 3
        results.outputCount == 2
        helper.count() == 2
        helper.find() == expectedResults
        serverUsed != null
    }

    @Category(Async)
    def 'should return the correct statistics and save the results asynchronously'() {
        given:
        def helper = new CollectionHelper<Document>(documentCodec, mapReduceOutputNamespace)
        def operation = new MapReduceToCollectionOperation(mapReduceInputNamespace, mapReduce)

        when:
        MapReduceStatistics results = operation.executeAsync(getAsyncBinding()).get()
        def serverUsed = operation.getServerUsed()

        then:
        results.emitCount == 3
        results.inputCount == 3
        results.outputCount == 2
        helper.count() == 2
        helper.find() == expectedResults
        serverUsed != null
    }

}
