/*
 * Copyright 2017 MongoDB, Inc.
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

package com.mongodb.client.model.changestream

import com.mongodb.MongoNamespace
import org.bson.BsonDocument
import org.bson.RawBsonDocument
import spock.lang.Specification

class ChangeStreamDocumentSpecification extends Specification {

    def 'should return the expected string value'() {
        given:
        def resumeToken = RawBsonDocument.parse('{token: true}')
        def namespace = new MongoNamespace('databaseName.collectionName')
        def fullDocument = BsonDocument.parse('{key: "value for fullDocument"}')
        def documentKey = BsonDocument.parse('{_id : 1}')
        def operationType = OperationType.UPDATE
        def updateDesc = new UpdateDescription(['a', 'b'], BsonDocument.parse('{c: 1}'))

        when:
        def changeStreamDocument = new ChangeStreamDocument<BsonDocument>(resumeToken, namespace, fullDocument, documentKey,
                operationType, updateDesc)

        then:
        changeStreamDocument.getResumeToken() == resumeToken
        changeStreamDocument.getFullDocument() == fullDocument
        changeStreamDocument.getDocumentKey() == documentKey
        changeStreamDocument.getNamespace() == namespace
        changeStreamDocument.getOperationType() == operationType
        changeStreamDocument.getUpdateDescription() == updateDesc
    }
}
