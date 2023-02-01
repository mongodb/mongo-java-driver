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

package com.mongodb.internal.connection

import com.mongodb.ClusterFixture
import com.mongodb.LoggerSettings
import com.mongodb.MongoCompressor
import com.mongodb.SubjectProvider
import com.mongodb.connection.ClusterId
import com.mongodb.connection.ServerId
import com.mongodb.connection.SocketSettings
import com.mongodb.connection.SocketStreamFactory
import spock.lang.IgnoreIf
import spock.lang.Specification

import javax.security.auth.login.LoginContext

import static com.mongodb.AuthenticationMechanism.GSSAPI
import static com.mongodb.ClusterFixture.getLoginContextName
import static com.mongodb.ClusterFixture.getPrimary
import static com.mongodb.ClusterFixture.getServerApi
import static com.mongodb.ClusterFixture.getSslSettings
import static com.mongodb.MongoCredential.JAVA_SUBJECT_PROVIDER_KEY
import static com.mongodb.connection.ClusterConnectionMode.SINGLE

@IgnoreIf({ ClusterFixture.getCredential() == null || ClusterFixture.getCredential().getAuthenticationMechanism() != GSSAPI })
class GSSAPIAuthenticatorSpecification extends Specification {

    def 'should use subject provider mechanism property'() {
        given:
        def loginContext = new LoginContext(getLoginContextName())
        loginContext.login()
        def subject = loginContext.getSubject()
        def subjectProvider = Mock(SubjectProvider)
        def credential = ClusterFixture.getCredential().withMechanismProperty(JAVA_SUBJECT_PROVIDER_KEY, subjectProvider)
        def credentialWithCache = new MongoCredentialWithCache(credential)
        def streamFactory = new SocketStreamFactory(SocketSettings.builder().build(), getSslSettings())
        def internalConnection = new InternalStreamConnectionFactory(SINGLE, streamFactory, credentialWithCache, null,
                null, Collections.<MongoCompressor> emptyList(), LoggerSettings.builder().build(), null, getServerApi())
                .create(new ServerId(new ClusterId(), getPrimary()))

        when:
        internalConnection.open()

        then:
        1 * subjectProvider.getSubject() >> subject
    }
}
