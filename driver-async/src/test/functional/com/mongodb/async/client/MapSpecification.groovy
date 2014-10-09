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
import com.mongodb.Function
import org.bson.Document
import org.bson.types.ObjectId

class MapSpecification extends FunctionalSpecification {

    def documents = [new Document('_id', new ObjectId()).append('x', 42), new Document('_id', new ObjectId()).append('x', 43)]

    def setup() {
        collection.insert(documents).get()
    }

    def 'should map source document into target document with into'() {
        expect:
        collection.find(new Document())
                  .map(new MappingFunction())
                  .into([]).get() == [new TargetDocument(documents[0]), new TargetDocument(documents[1])]
    }

    def 'should map source document into target document with forEach'() {
        when:
        def targetDocuments = []
        collection.find(new Document())
                  .map(new MappingFunction())
                  .forEach( { TargetDocument document -> targetDocuments += document } as Block<TargetDocument>).get()
        then:
        targetDocuments == [new TargetDocument(documents[0]), new TargetDocument(documents[1])]
    }

    def 'should map when already mapped'() {
        when:
        def targetIdStrings = []
        collection.find(new Document())
                  .map(new MappingFunction())
                  .map(new Function<TargetDocument, ObjectId>() {
            @Override
            ObjectId apply(final TargetDocument targetDocument) {
                targetDocument.getId()
            }
        }).forEach( { ObjectId id -> targetIdStrings += id.toString() } as Block<TargetDocument>).get()

        then:
        targetIdStrings == [new TargetDocument(documents[0]).getId().toString(), new TargetDocument(documents[1]).getId().toString()]
    }

    static class MappingFunction implements Function<Document, TargetDocument> {
        @Override
        TargetDocument apply(final Document document) {
            new TargetDocument(document)
        }
    }
}