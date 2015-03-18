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
import com.mongodb.MongoException
import com.mongodb.MongoExecutionTimeoutException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.async.FutureResultCallback
import com.mongodb.client.test.Worker
import com.mongodb.client.test.WorkerCodec
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonInvalidOperationException
import org.bson.Document
import org.bson.codecs.BsonValueCodecProvider
import org.bson.codecs.DocumentCodec
import org.bson.codecs.DocumentCodecProvider
import org.bson.codecs.ValueCodecProvider
import org.bson.types.ObjectId
import org.junit.experimental.categories.Category
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.disableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.enableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static java.util.Arrays.asList
import static java.util.concurrent.TimeUnit.SECONDS
import static org.bson.codecs.configuration.CodecRegistries.fromProviders

class DistinctOperationSpecification extends OperationFunctionalSpecification {

    def codecRegistry = fromProviders([new ValueCodecProvider(), new DocumentCodecProvider(), new BsonValueCodecProvider()])

    def getCodec(final Class clazz) {
        codecRegistry.get(clazz);
    }

    def stringDecoder = getCodec(String);

    def 'should be able to distinct by name'() {
        given:
        Document pete = new Document('name', 'Pete').append('age', 38)
        Document sam = new Document('name', 'Sam').append('age', 21)
        Document pete2 = new Document('name', 'Pete').append('age', 25)
        getCollectionHelper().insertDocuments(new DocumentCodec(), pete, sam, pete2)

        when:
        DistinctOperation op = new DistinctOperation(getNamespace(), 'name', stringDecoder)
        def result = op.execute(getBinding()).next();

        then:
        result == ['Pete', 'Sam']
    }

    @Category(Async)
    def 'should be able to distinct by name asynchronously'() {
        given:
        Document pete = new Document('name', 'Pete').append('age', 38)
        Document sam = new Document('name', 'Sam').append('age', 21)
        Document pete2 = new Document('name', 'Pete').append('age', 25)
        getCollectionHelper().insertDocuments(new DocumentCodec(), pete, sam, pete2)

        when:
        DistinctOperation op = new DistinctOperation(getNamespace(), 'name', stringDecoder)
        def futureResult = new FutureResultCallback()
        executeAsync(op).next(futureResult)
        def result = futureResult.get(60, SECONDS)

        then:
        result == ['Pete', 'Sam']
    }

    def 'should be able to distinct by name with find'() {
        given:
        Document pete = new Document('name', 'Pete').append('age', 38)
        Document sam = new Document('name', 'Sam').append('age', 21)
        Document pete2 = new Document('name', 'Pete').append('age', 25)
        getCollectionHelper().insertDocuments(new DocumentCodec(), pete, sam, pete2)

        when:
        DistinctOperation op = new DistinctOperation(getNamespace(), 'name', stringDecoder)
        op.filter(new BsonDocument('age', new BsonInt32(25)))
        def result = op.execute(getBinding());

        then:
        result.next() == ['Pete']
    }

    @Category(Async)
    def 'should be able to distinct by name with find asynchronously'() {
        given:
        Document pete = new Document('name', 'Pete').append('age', 38)
        Document sam = new Document('name', 'Sam').append('age', 21)
        Document pete2 = new Document('name', 'Pete').append('age', 25)
        getCollectionHelper().insertDocuments(new DocumentCodec(), pete, sam, pete2)

        when:
        DistinctOperation op = new DistinctOperation(getNamespace(), 'name', stringDecoder)
        op.filter(new BsonDocument('age', new BsonInt32(25)))
        def futureResult = new FutureResultCallback()
        executeAsync(op).next(futureResult)
        def result = futureResult.get(60, SECONDS)

        then:
        result == ['Pete']
    }

    def 'should be able to distinct with custom codecs'() {
        given:
        Worker pete = new Worker(new ObjectId(), 'Pete', 'handyman', new Date(), 3)
        Worker sam = new Worker(new ObjectId(), 'Sam', 'plumber', new Date(), 7)

        Document peteDocument = new Document('_id', pete.id)
                .append('name', pete.name)
                .append('jobTitle', pete.jobTitle)
                .append('dateStarted', pete.dateStarted)
                .append('numberOfJobs', pete.numberOfJobs)

        Document samDocument = new Document('_id', sam.id)
                .append('name', sam.name)
                .append('jobTitle', sam.jobTitle)
                .append('dateStarted', sam.dateStarted)
                .append('numberOfJobs', sam.numberOfJobs)

        getCollectionHelper().insertDocuments(new Document('worker', peteDocument), new Document('worker', samDocument));

        when:
        DistinctOperation op = new DistinctOperation(getNamespace(), 'worker', new WorkerCodec())
        def result = op.execute(getBinding()).next();

        then:
        result == [pete, sam]
    }

    @Category(Async)
    def 'should be able to distinct with custom codecs asynchronously'() {
        given:
        Worker pete = new Worker(new ObjectId(), 'Pete', 'handyman', new Date(), 3)
        Worker sam = new Worker(new ObjectId(), 'Sam', 'plumber', new Date(), 7)

        Document peteDocument = new Document('_id', pete.id)
                .append('name', pete.name)
                .append('jobTitle', pete.jobTitle)
                .append('dateStarted', pete.dateStarted)
                .append('numberOfJobs', pete.numberOfJobs)

        Document samDocument = new Document('_id', sam.id)
                .append('name', sam.name)
                .append('jobTitle', sam.jobTitle)
                .append('dateStarted', sam.dateStarted)
                .append('numberOfJobs', sam.numberOfJobs)

        getCollectionHelper().insertDocuments(new Document('worker', peteDocument), new Document('worker', samDocument));

        when:
        DistinctOperation op = new DistinctOperation(getNamespace(), 'worker', new WorkerCodec())
        def futureResult = new FutureResultCallback()
        executeAsync(op).next(futureResult)
        def result = futureResult.get(60, SECONDS)

        then:
        result == [pete, sam]
    }

    def 'should throw if invalid decoder passed to distinct'() {
        given:
        Document pete = new Document('name', 'Pete')
        Document sam = new Document('name', 1)
        Document pete2 = new Document('name', new Document('earle', 'Jones'))
        getCollectionHelper().insertDocuments(new DocumentCodec(), pete, sam, pete2)

        when:
        DistinctOperation op = new DistinctOperation(getNamespace(), 'name', stringDecoder)
        op.execute(getBinding()).next();

        then:
        thrown(BsonInvalidOperationException)
    }

    @Category(Async)
    def 'should throw if invalid decoder passed to distinct asynchronously'() {
        given:
        Document pete = new Document('name', 'Pete')
        Document sam = new Document('name', 1)
        Document pete2 = new Document('name', new Document('earle', 'Jones'))
        getCollectionHelper().insertDocuments(new DocumentCodec(), pete, sam, pete2)

        when:
        DistinctOperation op = new DistinctOperation(getNamespace(), 'name', stringDecoder)
        def futureResult = new FutureResultCallback()
        executeAsync(op).next(futureResult)
        futureResult.get(5, SECONDS)

        then:
        MongoException ex = thrown()
        ex.cause instanceof BsonInvalidOperationException
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(2, 6, 0)) })
    def 'should throw execution timeout exception from execute'() {
        given:
        def op = new DistinctOperation(getNamespace(), 'name', stringDecoder)
        op.maxTime(1, SECONDS)
        enableMaxTimeFailPoint()

        when:
        op.execute(getBinding())

        then:
        thrown(MongoExecutionTimeoutException)

        cleanup:
        disableMaxTimeFailPoint()
    }

    @Category(Async)
    @IgnoreIf({ !serverVersionAtLeast(asList(2, 6, 0)) })
    def 'should throw execution timeout exception from executeAsync'() {
        given:
        def op = new DistinctOperation(getNamespace(), 'name', stringDecoder)
        op.maxTime(1, SECONDS)
        enableMaxTimeFailPoint()

        when:
        executeAsync(op)

        then:
        thrown(MongoExecutionTimeoutException)

        cleanup:
        disableMaxTimeFailPoint()
    }
}
