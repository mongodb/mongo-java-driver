/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.model

import org.bson.BsonDocument
import spock.lang.Specification

class CreateCollectionOptionsSpecification extends Specification {

    def 'should have the expected defaults'() {
        when:
        def options = new CreateCollectionOptions()

        then:
        options.getCollation() == null
        options.getIndexOptionDefaults().getStorageEngine() == null
        options.getMaxDocuments() == 0
        options.getSizeInBytes() == 0
        options.getStorageEngineOptions() == null
        options.getValidationOptions().getValidator() == null
        options.isAutoIndex()
        !options.isCapped()
        !options.isUsePowerOf2Sizes()
   }

    def 'should set collation'() {
        expect:
        new CreateCollectionOptions().collation(collation).getCollation() == collation

        where:
        collation << [null, Collation.builder().locale('en').build()]
    }

    def 'should set indexOptionDefaults'() {
        expect:
        new CreateCollectionOptions().indexOptionDefaults(indexOptionDefaults).getIndexOptionDefaults() == indexOptionDefaults

        where:
        indexOptionDefaults << [null, new IndexOptionDefaults().storageEngine(BsonDocument.parse('{ storageEngine: { mmapv1 : {} }}'))]
    }

    def 'should set maxDocuments'() {
        expect:
        new CreateCollectionOptions().maxDocuments(maxDocuments).getMaxDocuments() == maxDocuments

        where:
        maxDocuments << [-1, 0, 1]
    }

    def 'should set sizeInBytes'() {
        expect:
        new CreateCollectionOptions().sizeInBytes(sizeInBytes).getSizeInBytes() == sizeInBytes

        where:
        sizeInBytes << [-1, 0, 1]
    }

    def 'should set storageEngineOptions'() {
        expect:
        new CreateCollectionOptions().storageEngineOptions(storageEngineOptions).getStorageEngineOptions() == storageEngineOptions

        where:
        storageEngineOptions << [null, BsonDocument.parse('{ mmapv1 : {} }')]
    }

    def 'should set validationOptions'() {
        expect:
        new CreateCollectionOptions().validationOptions(validationOptions).getValidationOptions() == validationOptions

        where:
        validationOptions << [new ValidationOptions(), new ValidationOptions().validationAction(ValidationAction.ERROR)]
    }

    def 'should set autoIndex'() {
        expect:
        new CreateCollectionOptions().autoIndex(autoIndex).isAutoIndex() == autoIndex

        where:
        autoIndex << [true, false]
    }

    def 'should set capped'() {
        expect:
        new CreateCollectionOptions().capped(capped).isCapped() == capped

        where:
        capped << [true, false]
    }

    def 'should set usePowerOf2Sizes'() {
        expect:
        new CreateCollectionOptions().usePowerOf2Sizes(usePowerOf2Sizes).isUsePowerOf2Sizes() == usePowerOf2Sizes

        where:
        usePowerOf2Sizes << [true, false]
    }
}
