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

package com.mongodb.connection

import com.mongodb.MongoSecurityException
import com.mongodb.ServerAddress
import com.mongodb.async.FutureResultCallback
import org.bson.BsonDocument
import org.bson.internal.Base64
import spock.lang.Specification

import javax.security.sasl.SaslException
import java.util.concurrent.TimeUnit

import static com.mongodb.MongoCredential.createScramSha1Credential
import static com.mongodb.MongoCredential.createScramSha256Credential
import static com.mongodb.connection.MessageHelper.buildSuccessfulReply

class ScramShaAuthenticatorSpecification extends Specification {
    def serverId = new ServerId(new ClusterId(), new ServerAddress('localhost', 27017))
    def connectionDescription = new ConnectionDescription(serverId)
    private final static MongoCredentialWithCache SHA1_CREDENTIAL =
            new MongoCredentialWithCache(createScramSha1Credential('user', 'database', 'pencil' as char[]))
    private final static MongoCredentialWithCache SHA256_CREDENTIAL =
            new MongoCredentialWithCache(createScramSha256Credential('user', 'database', 'pencil' as char[]))

    def 'should successfully authenticate with sha1 to RFC spec'() {
        when:
        def payloads = '''
            C: n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL
            S: r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096
            C: c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts=
            S: v=rmF9pqV8S7suAoZWja4dJRkFsKQ=
        '''
        def authenticator = new ScramShaAuthenticator(SHA1_CREDENTIAL, { 'fyko+d2lbbFgONRv9qkxdawL' }, { 'pencil' })

        then:
        validateAuthentication(payloads, authenticator, async)

        where:
        async << [true, false]
    }

    def 'should successfully authenticate with sha256 to RFC spec'() {
        when:
        def payloads = '''
            C: n,,n=user,r=rOprNGfwEbeRWgbNEkqO
            S: r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096
            C: c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=
            S: v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=
        '''
        def authenticator = new ScramShaAuthenticator(SHA256_CREDENTIAL, { 'rOprNGfwEbeRWgbNEkqO' }, { 'pencil' })

        then:
        validateAuthentication(payloads, authenticator, async)

        where:
        async << [true, false]
    }

    def 'should successfully authenticate with sha1 to MongoDB spec'() {
        when:
        def payloads = '''
            C: n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL
            S: r=fyko+d2lbbFgONRv9qkxdawLHo+Vgk7qvUOKUwuWLIWg4l/9SraGMHEE,s=rQ9ZY3MntBeuP3E1TDVC4w==,i=10000
            C: c=biws,r=fyko+d2lbbFgONRv9qkxdawLHo+Vgk7qvUOKUwuWLIWg4l/9SraGMHEE,p=MC2T8BvbmWRckDw8oWl5IVghwCY=
            S: v=UMWeI25JD1yNYZRMpZ4VHvhZ9e0=
        '''

        def authenticator = new ScramShaAuthenticator(SHA1_CREDENTIAL, { 'fyko+d2lbbFgONRv9qkxdawL' })

        then:
        validateAuthentication(payloads, authenticator, async)

        where:
        async << [true, false]
    }

    def 'should successfully authenticate with sha256 to MongoDB spec'() {
        when:
        def payloads = '''
            C: n,,n=user,r=rOprNGfwEbeRWgbNEkqO
            S: r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096
            C: c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=
            S: v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=
        '''
        def authenticator = new ScramShaAuthenticator(SHA256_CREDENTIAL, { 'rOprNGfwEbeRWgbNEkqO' })

        then:
        validateAuthentication(payloads, authenticator, async)

        where:
        async << [true, false]
    }

    def 'should prep username and password correctly for SHA1'() {
        when:
        def payloads = '''
            C: n,,n=ramo̒n,r=R815pGP84+H0OFRk+U/48qC+kwjw5TYS
            S: r=R815pGP84+H0OFRk+U/48qC+kwjw5TYSYjSeMWrU25u8Q73D9uM5aI4dxwOMaY3V,s=c2FsdA==,i=4096
            C: c=biws,r=R815pGP84+H0OFRk+U/48qC+kwjw5TYSYjSeMWrU25u8Q73D9uM5aI4dxwOMaY3V,p=Ib+1kvxT12Bj2FhVE68qtijgNfo=
            S: v=+cMTpXM1VzX5fEjtLXuNji5DeyA=
        '''

        def credential = new MongoCredentialWithCache(createScramSha1Credential('ramo\u0312n', 'database', 'p\u212Bssword' as char[]))
        def authenticator = new ScramShaAuthenticator(credential, { 'R815pGP84+H0OFRk+U/48qC+kwjw5TYS' })

        then:
        validateAuthentication(payloads, authenticator, async)

        where:
        async << [true, false]
    }

    def 'should prep username and password correctly for SHA256'() {
        when:
        def payloads = '''
            C: n,,n=u=2Cs1⁄2e ́rIX=3D,r=rOfhDB+wEbeRWgbNEkq9
            S: r=rOfhDB+wEbeRWgbNEkq9%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=QSXCR+Q6sek8bf92,i=4096
            C: c=biws,r=rOfhDB+wEbeRWgbNEkq9%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=+435koC4wp2/T9ORQmy75R13f1QGv9phV9LYQwssJZE=
            S: v=DKoN/Dii8S1ozDCVVJ7eAPHAe0KczTtxn2BsQtUeUgI=
        '''
        def credential = new MongoCredentialWithCache(
                createScramSha256Credential('u,s\u00BDe\u00B4r\u2168=', 'database', '\u2168pen\u00AAcil' as char[]))
        def authenticator = new ScramShaAuthenticator(credential, { 'rOfhDB+wEbeRWgbNEkq9' })

        then:
        validateAuthentication(payloads, authenticator, async)

        where:
        async << [true, false]
    }

    def 'should throw if invalid r value from server'() {
        when:
        def serverResponses = ['r=InvalidRValue,s=MYSALT,i=4096']
        def authenticator = new ScramShaAuthenticator(credential, { 'rOprNGfwEbeRWgbNEkqO' }, { 'pencil' })
        authenticate(createConnection(serverResponses), authenticator, async)

        then:
        def e = thrown(MongoSecurityException)
        e.getCause() instanceof SaslException
        e.getCause().getMessage() == 'Server sent an invalid nonce.'

        where:
        [async, credential] << [[true, false], [SHA1_CREDENTIAL, SHA256_CREDENTIAL]].combinations()
    }

    def 'should throw if iteration count is below the minimium allowed count'() {
        when:
        def serverResponses = createMessages('S: r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=QSXCR+Q6sek8bf92,i=4095').first()
        def authenticator = new ScramShaAuthenticator(credential, { 'rOprNGfwEbeRWgbNEkqO' }, { 'pencil' })
        authenticate(createConnection(serverResponses), authenticator, async)

        then:
        def e = thrown(MongoSecurityException)
        e.getCause() instanceof SaslException
        e.getCause().getMessage() == 'Invalid iteration count.'

        where:
        [async, credential] << [[true, false], [SHA1_CREDENTIAL, SHA256_CREDENTIAL]].combinations()
    }

    def 'should throw if invalid server signature'() {
        when:
        def serverResponses = createMessages('''
            S: r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=QSXCR+Q6sek8bf92,i=4096
            S: v=InvalidServerSignature
        ''').last()
        def authenticator = new ScramShaAuthenticator(credential, { 'rOprNGfwEbeRWgbNEkqO' }, { 'pencil' })
        authenticate(createConnection(serverResponses), authenticator, async)

        then:
        def e = thrown(MongoSecurityException)
        e.getCause() instanceof SaslException
        e.getCause().getMessage() == 'Server signature was invalid.'

        where:
        [async, credential] << [[true, false], [SHA1_CREDENTIAL, SHA256_CREDENTIAL]].combinations()
    }

    def 'should throw if too many steps SHA-1'() {
        when:
        def serverResponses = createMessages('''
            S: r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096
            S: v=rmF9pqV8S7suAoZWja4dJRkFsKQ=
            S: z=ExtraStep
        ''').last()
        def authenticator = new ScramShaAuthenticator(SHA1_CREDENTIAL, { 'fyko+d2lbbFgONRv9qkxdawL' }, { 'pencil' })
        authenticate(createConnection(serverResponses), authenticator, async)

        then:
        def e = thrown(MongoSecurityException)
        e.getCause() instanceof SaslException
        e.getCause().getMessage() == 'Too many steps involved in the SCRAM-SHA-1 negotiation.'

        where:
        async << [true, false]
    }

    def 'should throw if too many steps SHA-256'() {
        when:
        def serverResponses = createMessages('''
            S: r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096
            S: v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=
            S: z=ExtraStep
        ''').last()
        def authenticator = new ScramShaAuthenticator(SHA256_CREDENTIAL, { 'rOprNGfwEbeRWgbNEkqO' }, { 'pencil' })
        authenticate(createConnection(serverResponses), authenticator, async)

        then:
        def e = thrown(MongoSecurityException)
        e.getCause() instanceof SaslException
        e.getCause().getMessage() == 'Too many steps involved in the SCRAM-SHA-256 negotiation.'

        where:
        async << [true, false]
    }

    def createConnection(List<String> serverResponses) {
        TestInternalConnection connection = new TestInternalConnection(serverId)
        serverResponses.each {
            connection.enqueueReply(
                    buildSuccessfulReply("{conversationId: 1, payload: BinData(0, '${encode64(it)}'), done: false, ok: 1}")
            ) }
        connection.enqueueReply(buildSuccessfulReply('{conversationId: 1, done: true, ok: 1}'))
        connection
    }

    def validateClientMessages(TestInternalConnection connection, List<String> clientMessages, String mechanism) {
        def sent = connection.getSent().collect { MessageHelper.decodeCommand( it ) }
        assert(clientMessages.size() == sent.size())
        sent.indices.each {
            def sentMessage = sent.get(it)
            def messageStart = it == 0 ? "saslStart: 1, mechanism:'$mechanism'" : 'saslContinue: 1, conversationId: 1'
            def expectedMessage = BsonDocument.parse("{$messageStart, payload: BinData(0, '${encode64(clientMessages.get(it))}')}")
            assert(expectedMessage == sentMessage)
        }
    }

    def validateAuthentication(String payloads, ScramShaAuthenticator authenticator, boolean async) {
        def (clientMessages, serverResponses) = createMessages(payloads)
        def connection = createConnection(serverResponses)
        authenticate(connection, authenticator, async)
        validateClientMessages(connection, clientMessages, authenticator.getMechanismName())
    }

    def authenticate(TestInternalConnection connection, ScramShaAuthenticator authenticator, boolean async) {
        if (async) {
            FutureResultCallback<Void> futureCallback = new FutureResultCallback<Void>()
            authenticator.authenticateAsync(connection, connectionDescription, futureCallback)
            futureCallback.get(5, TimeUnit.SECONDS)
        } else {
            authenticator.authenticate(connection, connectionDescription)
        }
    }


    def encode64(String string) {
        Base64.encode(string.getBytes())
    }

    def createMessages(String messages) {
        def (clientMessages, serverResponses) = [[], []]
        def payloads = messages.stripMargin().readLines()*.trim().findAll { it.length() > 0 }
        payloads.each {
            def type = it[0..1]
            def message = it[2..-1].trim()

            if (type == 'C:') {
                clientMessages += message
            } else if (type == 'S:') {
                serverResponses += message
            } else {
                throw new IllegalArgumentException("Invalid message: $message")
            }
        }
        clientMessages += serverResponses.last()
        [clientMessages, serverResponses]
    }
}
