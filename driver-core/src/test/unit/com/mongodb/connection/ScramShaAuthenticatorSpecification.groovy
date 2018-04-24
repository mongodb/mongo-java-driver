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
import java.nio.charset.Charset
import java.util.concurrent.TimeUnit

import static com.mongodb.MongoCredential.createScramSha1Credential
import static com.mongodb.MongoCredential.createScramSha256Credential
import static com.mongodb.connection.MessageHelper.buildSuccessfulReply
import static org.junit.Assert.assertEquals

class ScramShaAuthenticatorSpecification extends Specification {
    def serverId = new ServerId(new ClusterId(), new ServerAddress('localhost', 27017))
    def connectionDescription = new ConnectionDescription(serverId)
    private final static MongoCredentialWithCache SHA1_CREDENTIAL =
            new MongoCredentialWithCache(createScramSha1Credential('user', 'database', 'pencil' as char[]))
    private final static MongoCredentialWithCache SHA256_CREDENTIAL =
            new MongoCredentialWithCache(createScramSha256Credential('user', 'database', 'pencil' as char[]))

    def 'should successfully authenticate with sha1 as per RFC spec'() {
        given:
        def user = 'user'
        def password = 'pencil'
        def preppedPassword = 'pencil'
        def payloads = '''
            C: n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL
            S: r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096
            C: c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts=
            S: v=rmF9pqV8S7suAoZWja4dJRkFsKQ=
        '''

        when:
        def credential = new MongoCredentialWithCache(createScramSha1Credential(user, 'database', password as char[]))
        def authenticator = new ScramShaAuthenticator(credential, { 'fyko+d2lbbFgONRv9qkxdawL' }, { preppedPassword })

        then:
        validateAuthentication(payloads, authenticator, async)

        where:
        async << [true, false]
    }

    def 'should successfully authenticate with sha256 as per RFC spec'() {
        given:
        def user = 'user'
        def password = 'pencil'
        def preppedPassword = 'pencil'
        def payloads = '''
            C: n,,n=user,r=rOprNGfwEbeRWgbNEkqO
            S: r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096
            C: c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=
            S: v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=
        '''

        when:
        def credential = new MongoCredentialWithCache(createScramSha256Credential(user, 'database', password as char[]))
        def authenticator = new ScramShaAuthenticator(credential, { 'rOprNGfwEbeRWgbNEkqO' }, { preppedPassword })

        then:
        validateAuthentication(payloads, authenticator, async)

        where:
        async << [true, false]
    }


    def 'should successfully authenticate with SHA-1 ASCII'() {
        given:
        def user = 'user'
        def password = 'pencil'
        def preppedPassword = 'pencil'
        def payloads = '''
            C: n,,n=user,r=clientNONCE
            S: r=clientNONCEserverNONCE,s=c2FsdFNBTFRzYWx0,i=4096
            C: c=biws,r=clientNONCEserverNONCE,p=I4oktcY7BOL0Agn0NlWRXlRP1mg=
            S: v=oKPvB1bE/9ydptJ+kohMgL+NdM0=
        '''

        when:
        def credential = new MongoCredentialWithCache(createScramSha1Credential(user, 'database', password as char[]))
        def authenticator = new ScramShaAuthenticator(credential, { 'clientNONCE' }, { preppedPassword })

        then:
        validateAuthentication(payloads, authenticator, async)

        where:
        async << [true, false]
    }

    def 'should successfully authenticate with SHA-1 ASCII user'() {
        given:
        def user = 'user'
        def password = 'p\u00e8ncil'
        def preppedPassword = 'p\u00e8ncil'
        def payloads = '''
            C: n,,n=user,r=clientNONCE
            S: r=clientNONCEserverNONCE,s=c2FsdFNBTFRzYWx0,i=4096
            C: c=biws,r=clientNONCEserverNONCE,p=yn797N2/XhIwZBB29LhEs6D6XVw=
            S: v=a6QRQikpGygizEM4/rCOvkgdglI=
        '''

        when:
        def credential = new MongoCredentialWithCache(createScramSha1Credential(user, 'database', password as char[]))
        def authenticator = new ScramShaAuthenticator(credential, { 'clientNONCE' }, { preppedPassword })

        then:
        validateAuthentication(payloads, authenticator, async)

        where:
        async << [true, false]
    }

    def 'should successfully authenticate with SHA-1 ASCII pass'() {
        given:
        def user = 'ram\u00f5n'
        def password = 'pencil'
        def preppedPassword = 'pencil'
        def payloads = '''
            C: n,,n=ram\u00f5n,r=clientNONCE
            S: r=clientNONCEserverNONCE,s=c2FsdFNBTFRzYWx0,i=4096
            C: c=biws,r=clientNONCEserverNONCE,p=kvH02DJiH7oHwk+SKpN4plfpF04=
            S: v=BoA2mAPlV/b9A5WPDbHmHZi3EGc=
        '''

        when:
        def credential = new MongoCredentialWithCache(createScramSha1Credential(user, 'database', password as char[]))
        def authenticator = new ScramShaAuthenticator(credential, { 'clientNONCE' }, { preppedPassword })

        then:
        validateAuthentication(payloads, authenticator, async)

        where:
        async << [true, false]
    }

    def 'should successfully authenticate with SHA-256 ASCII'(){
        given:
        def user ='user'
        def password ='pencil'
        def preppedPassword ='pencil'
        def payloads = '''
            C: n,,n=user,r=clientNONCE
            S: r=clientNONCEserverNONCE,s=c2FsdFNBTFRzYWx0,i=4096
            C: c=biws,r=clientNONCEserverNONCE,p=ItXnHvCDW7VGij6H+4rv2o93HvkLwrQaLkfVjeSMfrc=
            S: v=P61v8wxOu6B9J7Uij+Sk4zewSK1e6en6f5rCFO4OUNE=
        '''

        when:
        def credential = new MongoCredentialWithCache(createScramSha256Credential(user, 'database', password as char[]))
        def authenticator = new ScramShaAuthenticator(credential, { 'clientNONCE' }, { preppedPassword })

        then:
        validateAuthentication(payloads, authenticator, async)

        where:
        async << [true, false]
    }

    def 'should successfully authenticate with SHA-256 ASCII user'(){
        given:
        def user ='user'
        def password ='p\u00e8ncil'
        def preppedPassword ='p\u00e8ncil'
        def payloads = '''
            C: n,,n=user,r=clientNONCE
            S: r=clientNONCEserverNONCE,s=c2FsdFNBTFRzYWx0,i=4096
            C: c=biws,r=clientNONCEserverNONCE,p=o6rKPfQCKSGHClFxHjdSeiVCPA6K53++gpY3XlP8lI8=
            S: v=rsyNAwnHfclZKxAKx1tKfInH3xPVAzCy237DQo5n/N8=
        '''

        when:
        def credential = new MongoCredentialWithCache(createScramSha256Credential(user, 'database', password as char[]))
        def authenticator = new ScramShaAuthenticator(credential, { 'clientNONCE' }, { preppedPassword })

        then:
        validateAuthentication(payloads, authenticator, async)

        where:
        async << [true, false]
    }

    def 'should successfully authenticate with SHA-256 ASCII pass'(){
        given:
        def user ='ram\u00f5n'
        def password ='pencil'
        def preppedPassword ='pencil'
        def payloads = '''
            C: n,,n=ram\u00f5n,r=clientNONCE
            S: r=clientNONCEserverNONCE,s=c2FsdFNBTFRzYWx0,i=4096
            C: c=biws,r=clientNONCEserverNONCE,p=vRdD7SqiY5kMyAFX2enPOJK9BL+3YIVyuzCt1H2qc4o=
            S: v=sh7QPwVuquMatYobYpYOaPiNS+lqwTCmy3rdexRDDkE=
        '''

        when:
        def credential = new MongoCredentialWithCache(createScramSha256Credential(user, 'database', password as char[]))
        def authenticator = new ScramShaAuthenticator(credential, { 'clientNONCE' }, { preppedPassword })

        then:
        validateAuthentication(payloads, authenticator, async)

        where:
        async << [true, false]
    }

    def 'should successfully authenticate with SHA-256 SASLprep normal'(){
        given:
        def user ='ram\u00f5n'
        def password ='p\u00c5assword'
        def preppedPassword ='p\u00c5assword'
        def payloads = '''
            C: n,,n=ram\u00f5n,r=clientNONCE
            S: r=clientNONCEserverNONCE,s=c2FsdFNBTFRzYWx0,i=4096
            C: c=biws,r=clientNONCEserverNONCE,p=Km2zqmf/GbLdkItzscNI5D0c1f+GmLDi2fScTPm6d4k=
            S: v=30soY0l2BiInoDyrHxIuamz2LBvci1lFKo/tOMpqo98=
        '''

        when:
        def credential = new MongoCredentialWithCache(createScramSha256Credential(user, 'database', password as char[]))
        def authenticator = new ScramShaAuthenticator(credential, { 'clientNONCE' }, { preppedPassword })

        then:
        validateAuthentication(payloads, authenticator, async)

        where:
        async << [true, false]
    }

    def 'should successfully authenticate with SHA-256 SASLprep non-normal'(){
        given:
        def user ='ramo\u0301n'
        def password ='p\u212bssword'
        def preppedPassword ='p\u00c5ssword'
        def payloads = '''
            C: n,,n=ram\u00f3n,r=clientNONCE
            S: r=clientNONCEserverNONCE,s=c2FsdFNBTFRzYWx0,i=4096
            C: c=biws,r=clientNONCEserverNONCE,p=KXgIc8B+d5k3zx1P4rfs4TiybIlv11O85Jl1TrzEsfI=
            S: v=zG9u+MI5GPTROhnW/W1PUCKV4Uvp2SHzwFOZV9Hth/c=
        '''

        when:
        def credential = new MongoCredentialWithCache(createScramSha256Credential(user, 'database', password as char[]))
        def authenticator = new ScramShaAuthenticator(credential, { 'clientNONCE' }, { preppedPassword })

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
            assertEquals(expectedMessage, sentMessage)
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
        Base64.encode(string.getBytes(Charset.forName('UTF-8')))
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
