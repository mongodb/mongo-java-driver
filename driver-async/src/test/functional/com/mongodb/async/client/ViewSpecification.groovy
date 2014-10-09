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

import org.bson.Document

class ViewSpecification extends FunctionalSpecification {

    def sortedDocuments = []

    def setup() {
        (1..100).each {
            sortedDocuments += new Document('_id', it).append('x', 1)
        }
        collection.insert(sortedDocuments).get()
    }

    def 'one should return null if there are no matching documents'() {
        expect:
        collection.find(new Document('_id', 101)).one().get() == null
    }

    def 'one should return a document if there are a matching one'() {
        def document = collection.find(new Document('_id', 1)).sort(new Document('_id', 1)).one().get()
        expect:
        document == sortedDocuments[0]
    }

    def 'should sort documents'() {
        expect:
        collection.find(new Document()).sort(new Document('_id', 1)).into([]).get() == sortedDocuments
        collection.find(new Document()).sort(new Document('_id', -1)).into([]).get() == sortedDocuments.reverse()
    }

    def 'should skip documents'() {
        expect:
        collection.find(new Document()).sort(new Document('_id', 1))
                  .skip(90).into([]).get() == sortedDocuments.subList(90, 100)
    }

    def 'should limit documents'() {
        expect:
        collection.find(new Document()).sort(new Document('_id', 1))
                  .limit(90).into([]).get() == sortedDocuments.subList(0, 90)
    }

    def 'should only include requested fields'() {
        expect:
        collection.find(new Document()).sort(new Document('_id', 1)).fields(new Document('_id', 1)).one().get() == new Document('_id', 1)
    }
}