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

package com.mongodb.async.client

import com.mongodb.Block
import com.mongodb.WriteConcernResult
import com.mongodb.async.FutureResultCallback
import org.bson.Document

import static java.util.concurrent.TimeUnit.SECONDS

class ForEachSpecification extends FunctionalSpecification {
    def 'should complete with no results'() {
        expect:
        def futureResultCallback = new FutureResultCallback<Void>();
        collection.find(new Document()).forEach({ } as Block, futureResultCallback)
        futureResultCallback.get(60, SECONDS) == null
    }

    def 'should apply block and complete'() {
        given:
        def document = new Document()
        def futureResultCallback = new FutureResultCallback<WriteConcernResult>();
        collection.insertOne(document, futureResultCallback)
        futureResultCallback.get(60, SECONDS)

        when:
        def queriedDocuments = []
        futureResultCallback = new FutureResultCallback<Void>();
        collection.find(new Document()).forEach({ doc -> queriedDocuments += doc } as Block, futureResultCallback)
        futureResultCallback.get(60, SECONDS)

        then:
        queriedDocuments == [document]
    }

    def 'should apply block for each document and then complete'() {
        given:
        def documents = [new Document(), new Document()]
        def futureResultCallback = new FutureResultCallback<WriteConcernResult>();
        collection.insertMany([documents[0], documents[1]], futureResultCallback)
        futureResultCallback.get(60, SECONDS)

        when:
        def queriedDocuments = []
        futureResultCallback = new FutureResultCallback<Void>();
        collection.find(new Document()).forEach({ doc -> queriedDocuments += doc } as Block, futureResultCallback)
        futureResultCallback.get(60, SECONDS)

        then:
        queriedDocuments == documents
    }

    def 'should throw MongoInternalException if apply throws'() {
        given:
        def document = new Document()
        def futureResultCallback = new FutureResultCallback<WriteConcernResult>();
        collection.insertOne(document, futureResultCallback)
        futureResultCallback.get(60, SECONDS)

        when:
        futureResultCallback = new FutureResultCallback<Void>();
        collection.find(new Document()).forEach({ doc -> throw new IllegalArgumentException() } as Block, futureResultCallback)
        futureResultCallback.get(60, SECONDS)

        then:
        thrown(IllegalArgumentException)
    }
}
