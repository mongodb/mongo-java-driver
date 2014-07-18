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

package com.mongodb.async.rxjava.client

import org.mongodb.Document

import static Fixture.get
import static Fixture.getAsList

class ViewSpecification extends FunctionalSpecification {

    def sortedDocuments = []

    def setup() {
        (1..100).each {
            sortedDocuments += new Document('_id', it).append('x', 1)
        }
        get(collection.insert(sortedDocuments))
    }

    def 'one should return null if there are no matching documents'() {
        expect:
        get(collection.find(new Document('_id', 101)).one()) == null
    }

    def 'one should return a document if there are a matching one'() {
        expect:
        get(collection.find(new Document('_id', 1)).sort(new Document('_id', 1)).one()) == sortedDocuments[0]
    }

    def 'should sort documents'() {
        expect:
        getAsList(collection.find(new Document()).sort(new Document('_id', 1)).forEach()) == sortedDocuments
        getAsList(collection.find(new Document()).sort(new Document('_id', -1)).forEach()) == sortedDocuments.reverse()
    }

    def 'should skip documents'() {
        expect:
        getAsList(collection.find(new Document()).sort(new Document('_id', 1)).skip(90).forEach()) == sortedDocuments.subList(90, 100)
    }

    def 'should limit documents'() {
        expect:
        getAsList(collection.find(new Document()).sort(new Document('_id', 1)).limit(90).forEach()) == sortedDocuments.subList(0, 90)
    }

    def 'should only include requested fields'() {
        expect:
        get(collection.find(new Document()).sort(new Document('_id', 1)).fields(new Document('_id', 1)).one()) == new Document('_id', 1)
    }
}