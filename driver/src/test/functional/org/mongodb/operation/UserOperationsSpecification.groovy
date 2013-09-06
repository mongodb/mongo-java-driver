/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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















package org.mongodb.operation

import org.mongodb.Document
import org.mongodb.FunctionalSpecification
import org.mongodb.connection.MongoSecurityException
import org.mongodb.connection.NativeAuthenticationHelper
import spock.lang.Ignore

import static org.mongodb.Fixture.getBufferProvider
import static org.mongodb.Fixture.getPrimary
import static org.mongodb.Fixture.getSession

// TODO: This needs to be re-written because it relies on a non-public class.
@Ignore
class UserOperationsSpecification extends FunctionalSpecification {
    def userName = 'jeff'
    def password = '123'.toCharArray()

    def channelProvider

    def 'an added user should be found'() {
        given:
        def userDocument = new Document('user', userName)
                .append('pwd', NativeAuthenticationHelper.createAuthenticationHash(userName, password))
                .append('readOnly', true)

        when:
        new InsertUserOperation(getDatabaseName(), userDocument, getBufferProvider(), getSession(), true).execute()
        def foundUserDocument = new FindUserOperation(getDatabaseName(), getBufferProvider(), userName, getSession(), true).execute()

        then:
        foundUserDocument.get('user') == 'jeff'
        foundUserDocument.get('pwd') == NativeAuthenticationHelper.createAuthenticationHash(userName, password);
        foundUserDocument.get('readOnly') == true

        cleanup:
        new RemoveUserOperation(getDatabaseName(), userName, getBufferProvider(), getSession(), true).execute()
    }

    def 'an added user should authenticate'() {
        given:
        def userDocument = new Document('user', userName)
                .append('pwd', NativeAuthenticationHelper.createAuthenticationHash(userName, password))
                .append('readOnly', true);

        when:
        new InsertUserOperation(getDatabaseName(), userDocument, getBufferProvider(), getSession(), true).execute()

        then:
        channelProvider.create(getPrimary())

        cleanup:
        new RemoveUserOperation(getDatabaseName(), userName, getBufferProvider(), getSession(), true).execute()
    }

    def 'a removed user should not authenticate'() {
        given:
        def userDocument = new Document('user', userName)
                .append('pwd', NativeAuthenticationHelper.createAuthenticationHash(userName, password))
                .append('readOnly', true);
        new InsertUserOperation(getDatabaseName(), userDocument, getBufferProvider(), getSession(), true).execute()

        when:
        new RemoveUserOperation(getDatabaseName(), userName, getBufferProvider(), getSession(), true).execute()
        channelProvider.create(getPrimary())

        then:
        thrown(MongoSecurityException)
    }
}
