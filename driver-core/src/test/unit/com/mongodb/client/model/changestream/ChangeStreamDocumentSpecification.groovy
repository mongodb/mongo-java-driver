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

package com.mongodb.client.model.changestream

import com.mongodb.MongoNamespace
import org.bson.BsonDocument
import org.bson.BsonTimestamp
import org.bson.RawBsonDocument
import spock.lang.Specification

class ChangeStreamDocumentSpecification extends Specification {

    def 'should initialize correctly'() {
        given:
        def resumeToken = RawBsonDocument.parse('{token: true}')
        def namespaceDocument = BsonDocument.parse('{db: "databaseName", coll: "collectionName"}')
        def namespace = new MongoNamespace('databaseName.collectionName')
        def fullDocument = BsonDocument.parse('{key: "value for fullDocument"}')
        def documentKey = BsonDocument.parse('{_id : 1}')
        def clusterTime = new BsonTimestamp(1234, 2)
        def operationType = OperationType.UPDATE
        def updateDesc = new UpdateDescription(['a', 'b'], BsonDocument.parse('{c: 1}'))

        when:
        def changeStreamDocument = new ChangeStreamDocument<BsonDocument>(resumeToken, namespaceDocument, fullDocument,
                documentKey, clusterTime, operationType, updateDesc)

        then:
        changeStreamDocument.getResumeToken() == resumeToken
        changeStreamDocument.getFullDocument() == fullDocument
        changeStreamDocument.getDocumentKey() == documentKey
        changeStreamDocument.getClusterTime() == clusterTime
        changeStreamDocument.getNamespace() == namespace
        changeStreamDocument.getOperationType() == operationType
        changeStreamDocument.getUpdateDescription() == updateDesc
        changeStreamDocument.getDatabaseName() == namespace.getDatabaseName()
        changeStreamDocument.getNamespaceDocument() == namespaceDocument

        when:
        def changeStreamDocumentFromNamespace = new ChangeStreamDocument<BsonDocument>(resumeToken, namespace, fullDocument,
                documentKey, clusterTime, operationType, updateDesc)

        then:
        changeStreamDocumentFromNamespace.getResumeToken() == resumeToken
        changeStreamDocumentFromNamespace.getFullDocument() == fullDocument
        changeStreamDocumentFromNamespace.getDocumentKey() == documentKey
        changeStreamDocumentFromNamespace.getClusterTime() == clusterTime
        changeStreamDocumentFromNamespace.getNamespace() == namespace
        changeStreamDocumentFromNamespace.getOperationType() == operationType
        changeStreamDocumentFromNamespace.getUpdateDescription() == updateDesc
        changeStreamDocumentFromNamespace.getDatabaseName() == namespace.getDatabaseName()
        changeStreamDocumentFromNamespace.getNamespaceDocument() == namespaceDocument
    }

    def 'should handle null namespace correctly'() {
        given:
        def resumeToken = RawBsonDocument.parse('{token: true}')
        def fullDocument = BsonDocument.parse('{key: "value for fullDocument"}')
        def documentKey = BsonDocument.parse('{_id : 1}')
        def clusterTime = new BsonTimestamp(1234, 2)
        def operationType = OperationType.DROP_DATABASE
        def updateDesc = new UpdateDescription(['a', 'b'], BsonDocument.parse('{c: 1}'))
        def changeStreamDocumentNullNamespace = new ChangeStreamDocument<BsonDocument>(resumeToken, (BsonDocument) null,
                fullDocument, documentKey, clusterTime, operationType, updateDesc)

        expect:
        changeStreamDocumentNullNamespace.getDatabaseName() == null
        changeStreamDocumentNullNamespace.getNamespace() == null
        changeStreamDocumentNullNamespace.getNamespaceDocument() == null
    }

    def 'should return null on missing BsonDocument elements'() {
        given:
        def resumeToken = RawBsonDocument.parse('{token: true}')
        def namespaceDocument = BsonDocument.parse('{db: "databaseName"}')
        def namespaceDocumentEmpty = new BsonDocument()
        def fullDocument = BsonDocument.parse('{key: "value for fullDocument"}')
        def documentKey = BsonDocument.parse('{_id : 1}')
        def clusterTime = new BsonTimestamp(1234, 2)
        def operationType = OperationType.DROP_DATABASE
        def updateDesc = new UpdateDescription(['a', 'b'], BsonDocument.parse('{c: 1}'))

        def changeStreamDocument = new ChangeStreamDocument<BsonDocument>(resumeToken, namespaceDocument, fullDocument,
                documentKey, clusterTime, operationType, updateDesc)
        def changeStreamDocumentEmptyNamespace = new ChangeStreamDocument<BsonDocument>(resumeToken,
                namespaceDocumentEmpty, fullDocument, documentKey, clusterTime, operationType, updateDesc)

        expect:
        changeStreamDocument.getNamespace() == null
        changeStreamDocument.getDatabaseName() == 'databaseName'

        changeStreamDocumentEmptyNamespace.getNamespace() == null
        changeStreamDocumentEmptyNamespace.getDatabaseName() == null
    }
}
