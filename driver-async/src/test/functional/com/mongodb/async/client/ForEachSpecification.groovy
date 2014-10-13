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
import com.mongodb.MongoInternalException
import org.bson.types.Document

class ForEachSpecification extends FunctionalSpecification {
    def 'should complete with no results'() {
        expect:
        collection.find(new Document()).forEach( { } as Block).get() == null
    }

    def 'should apply block and complete'() {
        given:
        def document = new Document()
        collection.insert(document).get()

        when:
        def queriedDocuments = []
        collection.find(new Document()).forEach( { doc -> queriedDocuments += doc } as Block).get()

        then:
        queriedDocuments == [document]
    }

    def 'should apply block for each document and then complete'() {
        given:
        def documents = [new Document(), new Document()]
        collection.insert(documents[0]).get()
        collection.insert(documents[1]).get()

        when:
        def queriedDocuments = []
        collection.find(new Document()).forEach( { doc -> queriedDocuments += doc } as Block).get()

        then:
        queriedDocuments == documents
    }

    def 'should throw MongoInternalException if apply throws'() {
        given:
        def document = new Document()
        collection.insert(document).get()

        when:
        collection.find(new Document()).forEach( { doc -> throw new IllegalArgumentException() } as Block).get()

        then:
        thrown(MongoInternalException)
    }
}