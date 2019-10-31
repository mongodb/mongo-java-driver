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

package com.mongodb.reactivestreams.client.internal

import com.mongodb.MongoNamespace
import com.mongodb.ReadConcern
import com.mongodb.ReadPreference
import com.mongodb.WriteConcern
import com.mongodb.client.model.BulkWriteOptions
import com.mongodb.client.model.Collation
import com.mongodb.client.model.CollationStrength
import com.mongodb.client.model.CountOptions
import com.mongodb.client.model.CreateIndexOptions
import com.mongodb.client.model.DeleteOptions
import com.mongodb.client.model.DropIndexOptions
import com.mongodb.client.model.EstimatedDocumentCountOptions
import com.mongodb.client.model.FindOneAndDeleteOptions
import com.mongodb.client.model.FindOneAndReplaceOptions
import com.mongodb.client.model.FindOneAndUpdateOptions
import com.mongodb.client.model.IndexModel
import com.mongodb.client.model.IndexOptions
import com.mongodb.client.model.InsertManyOptions
import com.mongodb.client.model.InsertOneModel
import com.mongodb.client.model.InsertOneOptions
import com.mongodb.client.model.RenameCollectionOptions
import com.mongodb.client.model.ReplaceOptions
import com.mongodb.client.model.UpdateOptions
import com.mongodb.internal.async.client.AsyncAggregateIterable
import com.mongodb.internal.async.client.AsyncChangeStreamIterable
import com.mongodb.internal.async.client.AsyncClientSession as WrappedClientSession
import com.mongodb.internal.async.client.AsyncDistinctIterable
import com.mongodb.internal.async.client.AsyncFindIterable
import com.mongodb.internal.async.client.AsyncListIndexesIterable
import com.mongodb.internal.async.client.AsyncMapReduceIterable
import com.mongodb.internal.async.client.AsyncMongoCollection as WrappedMongoCollection
import com.mongodb.reactivestreams.client.ClientSession
import org.bson.BsonDocument
import org.bson.Document
import org.bson.codecs.configuration.CodecRegistry
import org.reactivestreams.Subscriber
import spock.lang.Specification

import static com.mongodb.reactivestreams.client.CustomMatchers.isTheSameAs
import static spock.util.matcher.HamcrestSupport.expect

@SuppressWarnings('ClassSize')
class MongoCollectionImplSpecification extends Specification {

    def subscriber = Stub(Subscriber) {
        onSubscribe(_) >> { args -> args[0].request(1) }
    }
    def wrappedClientSession = Stub(WrappedClientSession)
    def clientSession = Stub(ClientSession) {
        getWrapped() >> wrappedClientSession
    }
    def wrapped = Mock(WrappedMongoCollection)
    def mongoCollection = new MongoCollectionImpl(wrapped)
    def filter = new Document('_id', 1)
    def collation =  Collation.builder().locale('en').collationStrength(CollationStrength.SECONDARY).build()

    def 'should use the underlying getNamespace'() {
        when:
        mongoCollection.getNamespace()

        then:
        1 * wrapped.getNamespace()
    }

    def 'should use the underlying getDocumentClass'() {
        when:
        mongoCollection.getDocumentClass()

        then:
        1 * wrapped.getDocumentClass()
    }

    def 'should call the underlying getCodecRegistry'() {
        when:
        mongoCollection.getCodecRegistry()

        then:
        1 * wrapped.getCodecRegistry()
    }

    def 'should call the underlying getReadPreference'() {
        when:
        mongoCollection.getReadPreference()

        then:
        1 * wrapped.getReadPreference()
    }

    def 'should call the underlying getWriteConcern'() {
        when:
        mongoCollection.getWriteConcern()

        then:
        1 * wrapped.getWriteConcern()
    }

    def 'should call the underlying getReadConcern'() {
        when:
        mongoCollection.getReadConcern()

        then:
        1 * wrapped.getReadConcern()
    }

    def 'should use the underlying withDocumentClass'() {
        given:
        def wrappedResult = Stub(WrappedMongoCollection)
        def wrapped = Mock(WrappedMongoCollection) {
            1 * withDocumentClass(BsonDocument) >> wrappedResult
        }
        def mongoCollection = new MongoCollectionImpl(wrapped)

        when:
        def result = mongoCollection.withDocumentClass(BsonDocument)

        then:
        expect result, isTheSameAs(new MongoCollectionImpl(wrappedResult))
    }

    def 'should call the underlying withCodecRegistry'() {
        given:
        def codecRegistry = Stub(CodecRegistry)
        def wrappedResult = Stub(WrappedMongoCollection)
        def wrapped = Mock(WrappedMongoCollection) {
            1 * withCodecRegistry(codecRegistry) >> wrappedResult
        }
        def mongoCollection = new MongoCollectionImpl(wrapped)

        when:
        def result = mongoCollection.withCodecRegistry(codecRegistry)

        then:
        expect result, isTheSameAs(new MongoCollectionImpl(wrappedResult))
    }

    def 'should call the underlying withReadPreference'() {
        given:
        def readPreference = Stub(ReadPreference)
        def wrappedResult = Stub(WrappedMongoCollection)
        def wrapped = Mock(WrappedMongoCollection) {
            1 * withReadPreference(readPreference) >> wrappedResult
        }
        def mongoCollection = new MongoCollectionImpl(wrapped)

        when:
        def result = mongoCollection.withReadPreference(readPreference)

        then:
        expect result, isTheSameAs(new MongoCollectionImpl(wrappedResult))
    }

    def 'should call the underlying withWriteConcern'() {
        given:
        def writeConcern = Stub(WriteConcern)
        def wrappedResult = Stub(WrappedMongoCollection)
        def wrapped = Mock(WrappedMongoCollection) {
            1 * withWriteConcern(writeConcern) >> wrappedResult
        }
        def mongoCollection = new MongoCollectionImpl(wrapped)

        when:
        def result = mongoCollection.withWriteConcern(writeConcern)

        then:
        expect result, isTheSameAs(new MongoCollectionImpl(wrappedResult))
    }

    def 'should call the underlying withReadConcern'() {
        given:
        def readConcern = ReadConcern.MAJORITY
        def wrappedResult = Stub(WrappedMongoCollection)
        def wrapped = Mock(WrappedMongoCollection) {
            1 * withReadConcern(readConcern) >> wrappedResult
        }
        def mongoCollection = new MongoCollectionImpl(wrapped)

        when:
        def result = mongoCollection.withReadConcern(readConcern)

        then:
        expect result, isTheSameAs(new MongoCollectionImpl(wrappedResult))
    }

    def 'should use the underlying estimatedDocumentCount'() {
        given:
        def options = new EstimatedDocumentCountOptions()

        when:
        mongoCollection.estimatedDocumentCount()

        then: 'only executed when requested'
        0 * wrapped.estimatedDocumentCount(_, _, _)

        when:
        mongoCollection.estimatedDocumentCount().subscribe(subscriber)

        then:
        1 * wrapped.estimatedDocumentCount(_, _)

        when:
        mongoCollection.estimatedDocumentCount(options).subscribe(subscriber)

        then:
        1 * wrapped.estimatedDocumentCount(options, _)
    }

    def 'should use the underlying countDocuments'() {
        given:
        def options = new CountOptions()

        when:
        mongoCollection.countDocuments()

        then: 'only executed when requested'
        0 * wrapped.countDocuments(_, _, _)

        when:
        mongoCollection.countDocuments().subscribe(subscriber)

        then:
        1 * wrapped.countDocuments(_, _, _)

        when:
        mongoCollection.countDocuments(filter).subscribe(subscriber)

        then:
        1 * wrapped.countDocuments(filter, _, _)

        when:
        mongoCollection.countDocuments(filter, options).subscribe(subscriber)

        then:
        1 * wrapped.countDocuments(filter, options, _)

        when:
        mongoCollection.countDocuments(clientSession).subscribe(subscriber)

        then:
        1 * wrapped.countDocuments(wrappedClientSession, _, _, _)

        when:
        mongoCollection.countDocuments(clientSession, filter).subscribe(subscriber)

        then:
        1 * wrapped.countDocuments(wrappedClientSession, filter, _, _)

        when:
        mongoCollection.countDocuments(clientSession, filter, options).subscribe(subscriber)

        then:
        1 * wrapped.countDocuments(wrappedClientSession, filter, options, _)
    }

    def 'should create DistinctPublisher correctly'() {
        given:
        def wrapped = Stub(WrappedMongoCollection) {
            distinct(_, _) >> Stub(AsyncDistinctIterable)
            distinct(wrappedClientSession, _, _) >> Stub(AsyncDistinctIterable)
        }
        def collection = new MongoCollectionImpl(wrapped)

        when:
        def distinctPublisher = collection.distinct('field', String)

        then:
        expect distinctPublisher, isTheSameAs(new DistinctPublisherImpl(wrapped.distinct('field', String)))

        when:
        distinctPublisher = collection.distinct(clientSession, 'field', String)

        then:
        expect distinctPublisher, isTheSameAs(new DistinctPublisherImpl(wrapped.distinct(wrappedClientSession, 'field', String)))
    }

    def 'should create FindPublisher correctly'() {
        given:
        def wrappedResult = Stub(AsyncFindIterable)
        def wrapped = Mock(WrappedMongoCollection)
        def collection = new MongoCollectionImpl(wrapped)

        when:
        def findPublisher = collection.find()

        then:
        1 * wrapped.getDocumentClass() >> Document
        1 * wrapped.find(new BsonDocument(), Document) >> wrappedResult
        expect findPublisher, isTheSameAs(new FindPublisherImpl(wrappedResult))

        when:
        findPublisher = collection.find(BsonDocument)

        then:
        1 * wrapped.find(new BsonDocument(), BsonDocument) >> wrappedResult
        expect findPublisher, isTheSameAs(new FindPublisherImpl(wrappedResult))

        when:
        findPublisher = collection.find(new Document())

        then:
        1 * wrapped.getDocumentClass() >> Document
        1 * wrapped.find(new Document(), Document) >> wrappedResult
        expect findPublisher, isTheSameAs(new FindPublisherImpl(wrappedResult))

        when:
        findPublisher = collection.find(new Document(), BsonDocument)

        then:
        1 * wrapped.find(new Document(), BsonDocument) >> wrappedResult
        expect findPublisher, isTheSameAs(new FindPublisherImpl(wrappedResult))

        when:
        findPublisher = collection.find(clientSession)

        then:
        1 * wrapped.getDocumentClass() >> Document
        1 * wrapped.find(wrappedClientSession, new BsonDocument(), Document) >> wrappedResult
        expect findPublisher, isTheSameAs(new FindPublisherImpl(wrappedResult))

        when:
        findPublisher = collection.find(clientSession, BsonDocument)

        then:
        1 * wrapped.find(wrappedClientSession, new BsonDocument(), BsonDocument) >> wrappedResult
        expect findPublisher, isTheSameAs(new FindPublisherImpl(wrappedResult))

        when:
        findPublisher = collection.find(clientSession, new Document())

        then:
        1 * wrapped.getDocumentClass() >> Document
        1 * wrapped.find(wrappedClientSession, new Document(), Document) >> wrappedResult
        expect findPublisher, isTheSameAs(new FindPublisherImpl(wrappedResult))

        when:
        findPublisher = collection.find(new Document(), BsonDocument)

        then:
        1 * wrapped.find(new Document(), BsonDocument) >> wrappedResult
        expect findPublisher, isTheSameAs(new FindPublisherImpl(wrappedResult))
    }

    def 'should use AggregatePublisher correctly'() {
        given:
        def wrappedResult = Stub(AsyncAggregateIterable)
        def wrapped = Mock(WrappedMongoCollection)
        def collection = new MongoCollectionImpl(wrapped)
        def pipeline = [new Document('$match', 1)]

        when:
        def aggregatePublisher = collection.aggregate(pipeline)

        then:
        1 * wrapped.getDocumentClass() >> Document
        1 * wrapped.aggregate(pipeline, Document) >> wrappedResult
        expect aggregatePublisher, isTheSameAs(new AggregatePublisherImpl(wrappedResult))

        when:
        aggregatePublisher = collection.aggregate(pipeline, BsonDocument)

        then:
        1 * wrapped.aggregate(pipeline, BsonDocument) >> wrappedResult
        expect aggregatePublisher, isTheSameAs(new AggregatePublisherImpl(wrappedResult))

        when:
        aggregatePublisher = collection.aggregate(clientSession, pipeline)

        then:
        1 * wrapped.getDocumentClass() >> Document
        1 * wrapped.aggregate(wrappedClientSession, pipeline, Document) >> wrappedResult
        expect aggregatePublisher, isTheSameAs(new AggregatePublisherImpl(wrappedResult))

        when:
        aggregatePublisher = collection.aggregate(clientSession, pipeline, BsonDocument)

        then:
        1 * wrapped.aggregate(wrappedClientSession, pipeline, BsonDocument) >> wrappedResult
        expect aggregatePublisher, isTheSameAs(new AggregatePublisherImpl(wrappedResult))
    }

    def 'should use ChangeStreamPublisher correctly'() {
        given:
        def pipeline = [new Document('$match', 1)]
        def wrappedResult = Stub(AsyncChangeStreamIterable)
        def wrapped = Mock(WrappedMongoCollection)
        def collection = new MongoCollectionImpl(wrapped)
        def changeStreamPublisher

        when:
        changeStreamPublisher = collection.watch()

        then:
        1 * wrapped.watch([], Document) >> wrappedResult
        expect changeStreamPublisher, isTheSameAs(new ChangeStreamPublisherImpl(wrappedResult))

        when:
        changeStreamPublisher = collection.watch(BsonDocument)

        then:
        1 * wrapped.watch([], BsonDocument) >> wrappedResult
        expect changeStreamPublisher, isTheSameAs(new ChangeStreamPublisherImpl(wrappedResult))

        when:
        changeStreamPublisher = collection.watch(pipeline)

        then:
        1 * wrapped.watch(pipeline, Document) >> wrappedResult
        expect changeStreamPublisher, isTheSameAs(new ChangeStreamPublisherImpl(wrappedResult))

        when:
        changeStreamPublisher = collection.watch(pipeline, BsonDocument)

        then:
        1 * wrapped.watch(pipeline, BsonDocument) >> wrappedResult
        expect changeStreamPublisher, isTheSameAs(new ChangeStreamPublisherImpl(wrappedResult))

        when:
        changeStreamPublisher = collection.watch(clientSession)

        then:
        1 * wrapped.watch(wrappedClientSession, [], Document) >> wrappedResult
        expect changeStreamPublisher, isTheSameAs(new ChangeStreamPublisherImpl(wrappedResult))

        when:
        changeStreamPublisher = collection.watch(clientSession, BsonDocument)

        then:
        1 * wrapped.watch(wrappedClientSession, [], BsonDocument) >> wrappedResult
        expect changeStreamPublisher, isTheSameAs(new ChangeStreamPublisherImpl(wrappedResult))

        when:
        changeStreamPublisher = collection.watch(clientSession, pipeline)

        then:
        1 * wrapped.watch(wrappedClientSession, pipeline, Document) >> wrappedResult
        expect changeStreamPublisher, isTheSameAs(new ChangeStreamPublisherImpl(wrappedResult))

        when:
        changeStreamPublisher = collection.watch(clientSession, pipeline, BsonDocument)

        then:
        1 * wrapped.watch(wrappedClientSession, pipeline, BsonDocument) >> wrappedResult
        expect changeStreamPublisher, isTheSameAs(new ChangeStreamPublisherImpl(wrappedResult))
    }

    def 'should create MapReducePublisher correctly'() {
        given:
        def wrappedResult = Stub(AsyncMapReduceIterable)
        def wrapped = Mock(WrappedMongoCollection)
        def collection = new MongoCollectionImpl(wrapped)
        def mapReducePublisher

        when:
        mapReducePublisher = collection.mapReduce('map', 'reduce')

        then:
        1 * wrapped.getDocumentClass() >> Document
        1 * wrapped.mapReduce('map', 'reduce', Document) >> wrappedResult
        expect mapReducePublisher, isTheSameAs(new MapReducePublisherImpl(wrappedResult))

        when:
        mapReducePublisher = collection.mapReduce('map', 'reduce', BsonDocument)

        then:
        1 * wrapped.mapReduce('map', 'reduce', BsonDocument) >> wrappedResult
        expect mapReducePublisher, isTheSameAs(new MapReducePublisherImpl(wrappedResult))

        when:
        mapReducePublisher = collection.mapReduce(clientSession, 'map', 'reduce')

        then:
        1 * wrapped.getDocumentClass() >> Document
        1 * wrapped.mapReduce(wrappedClientSession, 'map', 'reduce', Document) >> wrappedResult
        expect mapReducePublisher, isTheSameAs(new MapReducePublisherImpl(wrappedResult))

        when:
        mapReducePublisher = collection.mapReduce(clientSession, 'map', 'reduce', BsonDocument)

        then:
        1 * wrapped.mapReduce(wrappedClientSession, 'map', 'reduce', BsonDocument) >> wrappedResult
        expect mapReducePublisher, isTheSameAs(new MapReducePublisherImpl(wrappedResult))
    }


    def 'should use the underlying bulkWrite'() {
        def subscriber = Stub(Subscriber) {
            onSubscribe(_) >> { args -> args[0].request(1) }
        }
        def bulkOperation = [new InsertOneModel<Document>(new Document('_id', 10))]
        def options = new BulkWriteOptions()

        when:
        mongoCollection.bulkWrite(bulkOperation)

        then: 'only executed when requested'
        0 * wrapped.bulkWrite(_, _, _)

        when:
        mongoCollection.bulkWrite(bulkOperation).subscribe(subscriber)

        then:
        1 * wrapped.bulkWrite(bulkOperation, _, _)

        when:
        mongoCollection.bulkWrite(bulkOperation, options).subscribe(subscriber)

        then:
        1 * wrapped.bulkWrite(bulkOperation, options, _)

        when:
        mongoCollection.bulkWrite(clientSession, bulkOperation).subscribe(subscriber)

        then:
        1 * wrapped.bulkWrite(wrappedClientSession, bulkOperation, _, _)

        when:
        mongoCollection.bulkWrite(clientSession, bulkOperation, options).subscribe(subscriber)

        then:
        1 * wrapped.bulkWrite(wrappedClientSession, bulkOperation, options, _)
    }

    def 'should use the underlying insertOne'() {
        given:
        def insert = new Document('_id', 1)
        def options = new InsertOneOptions()

        when:
        mongoCollection.insertOne(insert)

        then: 'only executed when requested'
        0 * wrapped.insertOne(_)

        when:
        mongoCollection.insertOne(insert).subscribe(subscriber)

        then:
        1 * wrapped.insertOne(insert, _, _)

        when:
        mongoCollection.insertOne(insert, options).subscribe(subscriber)

        then:
        1 * wrapped.insertOne(insert, options, _)

        when:
        mongoCollection.insertOne(clientSession, insert).subscribe(subscriber)

        then:
        1 * wrapped.insertOne(wrappedClientSession, insert, _, _)

        when:
        mongoCollection.insertOne(clientSession, insert, options).subscribe(subscriber)

        then:
        1 * wrapped.insertOne(wrappedClientSession, insert, options, _)
    }

    def 'should use the underlying insertMany'() {
        given:
        def inserts = [new Document('_id', 1)]
        def options = new InsertManyOptions()

        when:
        mongoCollection.insertMany(inserts)

        then: 'only executed when requested'
        0 * wrapped.insertMany(_, _, _)

        when:
        mongoCollection.insertMany(inserts).subscribe(subscriber)

        then:
        1 * wrapped.insertMany(inserts, _, _)

        when:
        mongoCollection.insertMany(inserts, options).subscribe(subscriber)

        then:
        1 * wrapped.insertMany(inserts, options, _)

        when:
        mongoCollection.insertMany(clientSession, inserts).subscribe(subscriber)

        then:
        1 * wrapped.insertMany(wrappedClientSession, inserts, _, _)

        when:
        mongoCollection.insertMany(clientSession, inserts, options).subscribe(subscriber)

        then:
        1 * wrapped.insertMany(wrappedClientSession, inserts, options, _)
    }

    def 'should use the underlying deleteOne'() {
        when:
        mongoCollection.deleteOne(filter)

        then: 'only executed when requested'
        0 * wrapped.deleteOne(*_)

        when:
        mongoCollection.deleteOne(filter).subscribe(subscriber)

        then:
        1 * wrapped.deleteOne(filter, _, _)

        when:
        mongoCollection.deleteOne(clientSession, filter).subscribe(subscriber)

        then:
        1 * wrapped.deleteOne(wrappedClientSession, filter, _, _)
    }

    def 'should use the underlying deleteOne with options'() {
        when:
        def options = new DeleteOptions().collation(collation)
        mongoCollection.deleteOne(filter, options)

        then: 'only executed when requested'
        0 * wrapped.deleteOne(_, _, _)

        when:
        mongoCollection.deleteOne(filter, options).subscribe(subscriber)

        then:
        1 * wrapped.deleteOne(filter, options, _)

        when:
        mongoCollection.deleteOne(clientSession, filter, options).subscribe(subscriber)

        then:
        1 * wrapped.deleteOne(wrappedClientSession, filter, options, _)
    }

    def 'should use the underlying deleteMany'() {
        when:
        mongoCollection.deleteMany(filter)

        then: 'only executed when requested'
        0 * wrapped.deleteMany(*_)

        when:
        mongoCollection.deleteMany(filter).subscribe(subscriber)

        then:
        1 * wrapped.deleteMany(filter, _, _)

        when:
        mongoCollection.deleteMany(clientSession, filter).subscribe(subscriber)

        then:
        1 * wrapped.deleteMany(wrappedClientSession, filter, _, _)
    }

    def 'should use the underlying deleteMany with options'() {
        when:
        def options = new DeleteOptions().collation(collation)
        mongoCollection.deleteMany(filter, options)

        then: 'only executed when requested'
        0 * wrapped.deleteMany(_, _, _)

        when:
        mongoCollection.deleteMany(filter, options).subscribe(subscriber)

        then:
        1 * wrapped.deleteMany(filter, options, _)

        when:
        mongoCollection.deleteMany(clientSession, filter, options).subscribe(subscriber)

        then:
        1 * wrapped.deleteMany(wrappedClientSession, filter, options, _)
    }

    def 'should use the underlying replaceOne'() {
        given:
        def replacement = new Document('new', 1)
        def replaceOptions = new ReplaceOptions()

        when:
        mongoCollection.replaceOne(filter, replacement)

        then: 'only executed when requested'
        0 * wrapped.replaceOne(_, _, _, _)

        when:
        mongoCollection.replaceOne(filter, replacement).subscribe(subscriber)

        then:
        1 * wrapped.replaceOne(filter, replacement, _, _)

        when:
        mongoCollection.replaceOne(filter, replacement, replaceOptions).subscribe(subscriber)

        then:
        1 * wrapped.replaceOne(filter, replacement, replaceOptions, _)

        when:
        mongoCollection.replaceOne(clientSession, filter, replacement).subscribe(subscriber)

        then:
        1 * wrapped.replaceOne(wrappedClientSession, filter, replacement, _, _)

        when:
        mongoCollection.replaceOne(clientSession, filter, replacement, replaceOptions).subscribe(subscriber)

        then:
        1 * wrapped.replaceOne(wrappedClientSession, filter, replacement, replaceOptions, _)
    }


    def 'should use the underlying updateOne'() {
        given:
        def options = new UpdateOptions()

        when:
        mongoCollection.updateOne(filter, update)

        then: 'only executed when requested'
        0 * wrapped.updateOne(_, _, _, _)

        when:
        mongoCollection.updateOne(filter, update).subscribe(subscriber)

        then:
        1 * wrapped.updateOne(filter, update, _, _)

        when:
        mongoCollection.updateOne(filter, update, options).subscribe(subscriber)

        then:
        1 * wrapped.updateOne(filter, update, options, _)

        when:
        mongoCollection.updateOne(clientSession, filter, update).subscribe(subscriber)

        then:
        1 * wrapped.updateOne(wrappedClientSession, filter, update, _, _)

        when:
        mongoCollection.updateOne(clientSession, filter, update, options).subscribe(subscriber)

        then:
        1 * wrapped.updateOne(wrappedClientSession, filter, update, options, _)

        where:
        update << [new Document('new', 1), [new Document('new', 1)]]
    }

    def 'should use the underlying updateMany'() {
        given:
        def options = new UpdateOptions()

        when:
        mongoCollection.updateMany(filter, update)

        then: 'only executed when requested'
        0 * wrapped.updateMany(_, _, _, _)

        when:
        mongoCollection.updateMany(filter, update).subscribe(subscriber)

        then:
        1 * wrapped.updateMany(filter, update, _, _)

        when:
        mongoCollection.updateMany(filter, update, options).subscribe(subscriber)

        then:
        1 * wrapped.updateMany(filter, update, options, _)

        when:
        mongoCollection.updateMany(clientSession, filter, update).subscribe(subscriber)

        then:
        1 * wrapped.updateMany(wrappedClientSession, filter, update, _, _)

        when:
        mongoCollection.updateMany(clientSession, filter, update, options).subscribe(subscriber)

        then:
        1 * wrapped.updateMany(wrappedClientSession, filter, update, options, _)

        where:
        update << [new Document('new', 1), [new Document('new', 1)]]
    }

    def 'should use the underlying findOneAndDelete'() {
        given:
        def options = new FindOneAndDeleteOptions()

        when:
        mongoCollection.findOneAndDelete(filter)

        then: 'only executed when requested'
        0 * wrapped.findOneAndDelete(_, _, _)

        when:
        mongoCollection.findOneAndDelete(filter).subscribe(subscriber)

        then:
        1 * wrapped.findOneAndDelete(filter, _, _)

        when:
        mongoCollection.findOneAndDelete(filter, options).subscribe(subscriber)

        then:
        1 * wrapped.findOneAndDelete(filter, options, _)

        when:
        mongoCollection.findOneAndDelete(clientSession, filter).subscribe(subscriber)

        then:
        1 * wrapped.findOneAndDelete(wrappedClientSession, filter, _, _)

        when:
        mongoCollection.findOneAndDelete(clientSession, filter, options).subscribe(subscriber)

        then:
        1 * wrapped.findOneAndDelete(wrappedClientSession, filter, options, _)
    }

    def 'should use the underlying findOneAndReplace'() {
        given:
        def replacement = new Document('new', 1)
        def options = new FindOneAndReplaceOptions()

        when:
        mongoCollection.findOneAndReplace(filter, replacement)

        then: 'only executed when requested'
        0 * wrapped.findOneAndReplace(_, _, _, _)

        when:
        mongoCollection.findOneAndReplace(filter, replacement).subscribe(subscriber)

        then:
        1 * wrapped.findOneAndReplace(filter, replacement, _, _)

        when:
        mongoCollection.findOneAndReplace(filter, replacement, options).subscribe(subscriber)

        then:
        1 * wrapped.findOneAndReplace(filter, replacement, options, _)

        when:
        mongoCollection.findOneAndReplace(clientSession, filter, replacement).subscribe(subscriber)

        then:
        1 * wrapped.findOneAndReplace(wrappedClientSession, filter, replacement, _, _)

        when:
        mongoCollection.findOneAndReplace(clientSession, filter, replacement, options).subscribe(subscriber)

        then:
        1 * wrapped.findOneAndReplace(wrappedClientSession, filter, replacement, options, _)
    }

    def 'should use the underlying findOneAndUpdate'() {
        given:
        def options = new FindOneAndUpdateOptions()

        when:
        mongoCollection.findOneAndUpdate(filter, update)

        then: 'only executed when requested'
        0 * wrapped.findOneAndUpdate(_, _, _, _)

        when:
        mongoCollection.findOneAndUpdate(filter, update).subscribe(subscriber)

        then:
        1 * wrapped.findOneAndUpdate(filter, update, _, _)

        when:
        mongoCollection.findOneAndUpdate(filter, update, options).subscribe(subscriber)

        then:
        1 * wrapped.findOneAndUpdate(filter, update, options, _)

        when:
        mongoCollection.findOneAndUpdate(clientSession, filter, update).subscribe(subscriber)

        then:
        1 * wrapped.findOneAndUpdate(wrappedClientSession, filter, update, _, _)

        when:
        mongoCollection.findOneAndUpdate(clientSession, filter, update, options).subscribe(subscriber)

        then:
        1 * wrapped.findOneAndUpdate(wrappedClientSession, filter, update, options, _)

        where:
        update << [new Document('new', 1), [new Document('new', 1)]]
    }

    def 'should use the underlying drop'() {
        when:
        mongoCollection.drop()

        then: 'only executed when requested'
        0 * wrapped.drop(_)

        when:
        mongoCollection.drop().subscribe(subscriber)

        then:
        1 * wrapped.drop(_)

        when:
        mongoCollection.drop(clientSession).subscribe(subscriber)

        then:
        1 * wrapped.drop(wrappedClientSession, _)
    }

    def 'should use the underlying createIndex'() {
        given:
        def index = new Document('index', 1)
        def options = new IndexOptions()

        when:
        mongoCollection.createIndex(index)

        then: 'only executed when requested'
        0 * wrapped.createIndex(_, _, _)

        when:
        mongoCollection.createIndex(index).subscribe(subscriber)

        then:
        1 * wrapped.createIndex(index, _, _)

        when:
        mongoCollection.createIndex(index, options).subscribe(subscriber)

        then:
        1 * wrapped.createIndex(index, options, _)

        when:
        mongoCollection.createIndex(clientSession, index).subscribe(subscriber)

        then:
        1 * wrapped.createIndex(wrappedClientSession, index, _, _)

        when:
        mongoCollection.createIndex(clientSession, index, options).subscribe(subscriber)

        then:
        1 * wrapped.createIndex(wrappedClientSession, index, options, _)
    }

    def 'should use the underlying createIndexes'() {
        given:
        def indexes = [new IndexModel(new Document('index', 1))]
        def options = new CreateIndexOptions()

        when:
        mongoCollection.createIndexes(indexes)

        then: 'only executed when requested'
        0 * wrapped.createIndexes(*_)

        when:
        mongoCollection.createIndexes(indexes).subscribe(subscriber)

        then:
        1 * wrapped.createIndexes(indexes, _, _)

        when:
        mongoCollection.createIndexes(indexes, options).subscribe(subscriber)

        then:
        1 * wrapped.createIndexes(indexes, options, _)

        when:
        mongoCollection.createIndexes(clientSession, indexes).subscribe(subscriber)

        then:
        1 * wrapped.createIndexes(wrappedClientSession, indexes, _, _)

        when:
        mongoCollection.createIndexes(clientSession, indexes, options).subscribe(subscriber)

        then:
        1 * wrapped.createIndexes(wrappedClientSession, indexes, options, _)
    }

    def 'should use the underlying listIndexes'() {
        def listIndexesIterable = Stub(AsyncListIndexesIterable)
        def wrapped = Mock(WrappedMongoCollection)
        def mongoCollection = new MongoCollectionImpl(wrapped)

        when:
        def publisher = mongoCollection.listIndexes()

        then:
        1 * wrapped.listIndexes(_) >> listIndexesIterable
        expect publisher, isTheSameAs(new ListIndexesPublisherImpl(listIndexesIterable))

        when:
        mongoCollection.listIndexes(BsonDocument)

        then:
        1 * wrapped.listIndexes(BsonDocument) >> listIndexesIterable
        expect publisher, isTheSameAs(new ListIndexesPublisherImpl(listIndexesIterable))

        when:
        publisher = mongoCollection.listIndexes(clientSession)

        then:
        1 * wrapped.listIndexes(wrappedClientSession, _) >> listIndexesIterable
        expect publisher, isTheSameAs(new ListIndexesPublisherImpl(listIndexesIterable))

        when:
        mongoCollection.listIndexes(clientSession, BsonDocument)

        then:
        1 * wrapped.listIndexes(wrappedClientSession, BsonDocument) >> listIndexesIterable
        expect publisher, isTheSameAs(new ListIndexesPublisherImpl(listIndexesIterable))
    }

    def 'should use the underlying dropIndex'() {
        given:
        def index = 'index'
        def dropIndexOptions = new DropIndexOptions()

        when:
        mongoCollection.dropIndex(index)

        then: 'only executed when requested'
        0 * wrapped.dropIndex(*_)

        when:
        mongoCollection.dropIndex(index).subscribe(subscriber)

        then:
        1 * wrapped.dropIndex(index, _, _)

        when:
        index = new Document('index', 1)
        mongoCollection.dropIndex(index).subscribe(subscriber)

        then:
        1 * wrapped.dropIndex(_, _, _)

        when:
        mongoCollection.dropIndexes().subscribe(subscriber)

        then:
        1 * wrapped.dropIndex('*', _, _)

        when:
        mongoCollection.dropIndex(index, dropIndexOptions).subscribe(subscriber)

        then:
        1 * wrapped.dropIndex(index, dropIndexOptions, _)

        when:
        index = new Document('index', 1)
        mongoCollection.dropIndex(index, dropIndexOptions).subscribe(subscriber)

        then:
        1 * wrapped.dropIndex(index, dropIndexOptions, _)

        when:
        mongoCollection.dropIndexes(dropIndexOptions).subscribe(subscriber)

        then:
        1 * wrapped.dropIndex('*', dropIndexOptions, _)

        when:
        mongoCollection.dropIndex(clientSession, index).subscribe(subscriber)

        then:
        1 * wrapped.dropIndex(wrappedClientSession, index, _, _)

        when:
        index = new Document('index', 1)
        mongoCollection.dropIndex(clientSession, index).subscribe(subscriber)

        then:
        1 * wrapped.dropIndex(wrappedClientSession, index, _, _)

        when:
        mongoCollection.dropIndexes(clientSession).subscribe(subscriber)

        then:
        1 * wrapped.dropIndex(wrappedClientSession, '*', _, _)

        when:
        mongoCollection.dropIndex(clientSession, index, dropIndexOptions).subscribe(subscriber)

        then:
        1 * wrapped.dropIndex(wrappedClientSession, index, dropIndexOptions, _)

        when:
        index = new Document('index', 1)
        mongoCollection.dropIndex(clientSession, index, dropIndexOptions).subscribe(subscriber)

        then:
        1 * wrapped.dropIndex(wrappedClientSession, index, dropIndexOptions, _)

        when:
        mongoCollection.dropIndexes(clientSession, dropIndexOptions).subscribe(subscriber)

        then:
        1 * wrapped.dropIndex(wrappedClientSession, '*', dropIndexOptions, _)
    }

    def 'should use the underlying renameCollection'() {
        given:
        def nameCollectionNamespace = new MongoNamespace('db', 'coll')
        def options = new RenameCollectionOptions()

        when:
        mongoCollection.renameCollection(nameCollectionNamespace)

        then: 'only executed when requested'
        0 * wrapped.renameCollection(_, _, _)

        when:
        mongoCollection.renameCollection(nameCollectionNamespace).subscribe(subscriber)

        then:
        1 * wrapped.renameCollection(nameCollectionNamespace, _, _)

        when:
        mongoCollection.renameCollection(nameCollectionNamespace, options).subscribe(subscriber)

        then:
        1 * wrapped.renameCollection(nameCollectionNamespace, options, _)

        when:
        mongoCollection.renameCollection(clientSession, nameCollectionNamespace).subscribe(subscriber)

        then:
        1 * wrapped.renameCollection(wrappedClientSession, nameCollectionNamespace, _, _)

        when:
        mongoCollection.renameCollection(clientSession, nameCollectionNamespace, options).subscribe(subscriber)

        then:
        1 * wrapped.renameCollection(wrappedClientSession, nameCollectionNamespace, options, _)
    }

}
