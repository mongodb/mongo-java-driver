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

package com.mongodb.async.rx.client

import org.bson.Document

import static com.mongodb.async.rx.client.Helpers.get
import static com.mongodb.async.rx.client.Helpers.toList

class ToObservableSpecification extends FunctionalSpecification {
    def 'should complete with no results'() {
        expect:
        toList(collection.find(new Document()).toObservable()) == []
    }

    def 'should apply block and complete'() {
        given:
        def document = new Document()
        get(collection.insertOne(document))

        when:
        def queriedDocuments = toList(collection.find(new Document()).toObservable())

        then:
        queriedDocuments == [document]
    }

    def 'should apply block for each document and then complete'() {
        given:
        def documents = [new Document(), new Document()]
        get(collection.insertOne(documents[0]))
        get(collection.insertOne(documents[1]))

        when:
        def queriedDocuments = toList(collection.find(new Document()).toObservable())

        then:
        queriedDocuments == documents
    }

}