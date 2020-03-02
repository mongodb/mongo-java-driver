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

import com.mongodb.MongoSecurityException
import com.mongodb.ServerAddress
import com.mongodb.async.FutureResultCallback
import com.mongodb.connection.ClusterId
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.ServerId
import org.bson.BsonDocument
import org.bson.internal.Base64
import spock.lang.Specification

import javax.security.sasl.SaslException
import java.nio.charset.Charset
import java.util.concurrent.TimeUnit

import static com.mongodb.MongoCredential.createScramSha1Credential
import static com.mongodb.MongoCredential.createScramSha256Credential
import static org.junit.Assert.assertEquals
import static com.mongodb.internal.connection.MessageHelper.buildSuccessfulReply

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
        validateAuthentication(payloads, authenticator, async, emptyExchange)

        where:
        [async, emptyExchange] << [[true, false], [true, false]].combinations()
    }

    def 'should speculatively authenticate with sha1'() {
        given:
        def user = 'user'
        def password = 'pencil'
        def preppedPassword = 'pencil'
        def payloads = '''
            C: c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts=
            S: v=rmF9pqV8S7suAoZWja4dJRkFsKQ=
        '''
        def firstClientChallenge = 'n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL'
        def expectedSpeculativeAuthenticateCommand = BsonDocument.parse('{ saslStart: 1, mechanism: "SCRAM-SHA-1", '
                + "payload: BinData(0, '${encode64(firstClientChallenge)}'), "
                + 'db: "admin", options: { skipEmptyExchange: true }}')
        def serverResponse = 'r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096'
        def speculativeAuthenticateResponse =
                BsonDocument.parse("{ conversationId: 1, payload: BinData(0, '${encode64(serverResponse)}'), done: false }")

        when:
        def credential = new MongoCredentialWithCache(createScramSha1Credential(user, 'database', password as char[]))
        def authenticator = new ScramShaAuthenticator(credential, { 'fyko+d2lbbFgONRv9qkxdawL' }, { preppedPassword })

        then:
        def speculativeAuthenticateCommand =
                validateSpeculativeAuthentication(payloads, authenticator, async, speculativeAuthenticateResponse)
        ((SpeculativeAuthenticator) authenticator).getSpeculativeAuthenticateResponse() == speculativeAuthenticateResponse
        speculativeAuthenticateCommand.equals(expectedSpeculativeAuthenticateCommand)

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
        validateAuthentication(payloads, authenticator, async, emptyExchange)

        where:
        [async, emptyExchange] << [[true, false], [true, false]].combinations()
    }

    def 'should speculatively authenticate with sha256'() {
        given:
        def user = 'user'
        def password = 'pencil'
        def preppedPassword = 'pencil'
        def payloads = '''
            C: c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=
            S: v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=
        '''
        def firstClientChallenge = 'n,,n=user,r=rOprNGfwEbeRWgbNEkqO'
        def expectedSpeculativeAuthenticateCommand = BsonDocument.parse('{ saslStart: 1, mechanism: "SCRAM-SHA-256", '
                + "payload: BinData(0, '${encode64(firstClientChallenge)}'), "
                + 'db: "admin", options: { skipEmptyExchange: true }}')
        def serverResponse = 'r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096'
        def speculativeAuthenticateResponse =
                BsonDocument.parse("{ conversationId: 1, payload: BinData(0, '${encode64(serverResponse)}'), done: false }")

        when:
        def credential = new MongoCredentialWithCache(createScramSha256Credential(user, 'database', password as char[]))
        def authenticator = new ScramShaAuthenticator(credential, { 'rOprNGfwEbeRWgbNEkqO' }, { preppedPassword })

        then:
        def speculativeAuthenticateCommand =
                validateSpeculativeAuthentication(payloads, authenticator, async, speculativeAuthenticateResponse)
        ((SpeculativeAuthenticator) authenticator).getSpeculativeAuthenticateResponse() == speculativeAuthenticateResponse
        speculativeAuthenticateCommand.equals(expectedSpeculativeAuthenticateCommand)

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
        validateAuthentication(payloads, authenticator, async, emptyExchange)

        where:
        [async, emptyExchange] << [[true, false], [true, false]].combinations()
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
        validateAuthentication(payloads, authenticator, async, emptyExchange)

        where:
        [async, emptyExchange] << [[true, false], [true, false]].combinations()
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
        validateAuthentication(payloads, authenticator, async, emptyExchange)

        where:
        [async, emptyExchange] << [[true, false], [true, false]].combinations()
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
        validateAuthentication(payloads, authenticator, async, emptyExchange)

        where:
        [async, emptyExchange] << [[true, false], [true, false]].combinations()
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
        validateAuthentication(payloads, authenticator, async, emptyExchange)

        where:
        [async, emptyExchange] << [[true, false], [true, false]].combinations()
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
        validateAuthentication(payloads, authenticator, async, emptyExchange)

        where:
        [async, emptyExchange] << [[true, false], [true, false]].combinations()
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
        validateAuthentication(payloads, authenticator, async, emptyExchange)

        where:
        [async, emptyExchange] << [[true, false], [true, false]].combinations()
    }

    def 'should successfully authenticate with SHA-256 SASLprep non-normal'(){
        given:
        def user ='ramo\u0301n'
        def password ='p\u212bssword'
        def preppedPassword ='p\u00c5ssword'
        def payloads = '''
            C: n,,n=ramo\u0301n,r=clientNONCE
            S: r=clientNONCEserverNONCE,s=c2FsdFNBTFRzYWx0,i=4096
            C: c=biws,r=clientNONCEserverNONCE,p=KkLV/eEHHw0LrTlnmElWuTiL0RxDa8lF/RqzsDP04sE=
            S: v=eLTDerRxJFOBV8+/9xOcIkv4PezVAcNAarSyqa5mQyI=
        '''

        when:
        def credential = new MongoCredentialWithCache(createScramSha256Credential(user, 'database', password as char[]))
        def authenticator = new ScramShaAuthenticator(credential, { 'clientNONCE' }, { preppedPassword })

        then:
        validateAuthentication(payloads, authenticator, async, emptyExchange)

        where:
        [async, emptyExchange] << [[true, false], [true, false]].combinations()
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
        def serverResponses = createMessages('S: r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=QSXCR+Q6sek8bf92,i=4095').last()
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
        ''', true).last()
        def authenticator = new ScramShaAuthenticator(SHA256_CREDENTIAL, { 'rOprNGfwEbeRWgbNEkqO' }, { 'pencil' })
        authenticate(createConnection(serverResponses), authenticator, async)

        then:
        def e = thrown(MongoSecurityException)
        e.getCause() instanceof SaslException
        e.getCause().getMessage() == 'Too many steps involved in the SCRAM-SHA-256 negotiation.'

        where:
        async << [true, false]
    }

    def 'should complete authentication when done is set to true prematurely SHA-256'() {
        given:
        def serverResponses = createMessages('''
            S: r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096
            S: v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=
        ''').last()
        def authenticator = new ScramShaAuthenticator(SHA256_CREDENTIAL, { 'rOprNGfwEbeRWgbNEkqO' }, { 'pencil' })

        when:
        // server sends done=true on first response, client is not complete after processing response
        authenticate(createConnection(serverResponses, 0), authenticator, async)

        then:
        def e = thrown(MongoSecurityException)
        e.getMessage().contains('server completed challenges before client completed responses')

        when:
        // server sends done=true on second response, client is complete after processing response
        authenticate(createConnection(serverResponses, 1), authenticator, async)

        then:
        noExceptionThrown()

        where:
        async << [true, false]
    }

    def 'should throw exception when done is set to true prematurely and server response is invalid SHA-256'() {
        given:
        def serverResponses = createMessages('''
            S: r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096
            S: v=invalidResponse
        ''').last()
        def authenticator = new ScramShaAuthenticator(SHA256_CREDENTIAL, { 'rOprNGfwEbeRWgbNEkqO' }, { 'pencil' })

        when:
        // server sends done=true on second response, client throws exception on invalid server response
        authenticate(createConnection(serverResponses, 1), authenticator, async)

        then:
        def e = thrown(MongoSecurityException)
        e.getCause() instanceof SaslException
        e.getCause().getMessage() == 'Server signature was invalid.'

        where:
        async << [true, false]
    }

    def createConnection(List<String> serverResponses, int responseWhereDoneIsTrue = -1) {
        TestInternalConnection connection = new TestInternalConnection(serverId)
        serverResponses.eachWithIndex { response, index ->
            def isDone = (index == responseWhereDoneIsTrue).booleanValue()
            connection.enqueueReply(
                    buildSuccessfulReply("{conversationId: 1, payload: BinData(0, '${encode64(response)}'), done: ${isDone}, ok: 1}")
            )
        }
        if (responseWhereDoneIsTrue < 0) {
            connection.enqueueReply(buildSuccessfulReply('{conversationId: 1, done: true, ok: 1}'))
        }
        connection
    }

    def validateClientMessages(TestInternalConnection connection, List<String> clientMessages, String mechanism,
                               boolean speculativeAuthenticate = false) {
        def sent = connection.getSent().collect { MessageHelper.decodeCommand( it ) }
        assert(clientMessages.size() == sent.size())
        sent.indices.each {
            def sentMessage = sent.get(it)
            def messageStart = speculativeAuthenticate || it != 0 ? 'saslContinue: 1, conversationId: 1'
                    : "saslStart: 1, mechanism:'$mechanism', options: {skipEmptyExchange: true}"
            def expectedMessage = BsonDocument.parse("{$messageStart, payload: BinData(0, '${encode64(clientMessages.get(it))}')}")
            assertEquals(expectedMessage, sentMessage)
        }
    }

    def validateAuthentication(String payloads, ScramShaAuthenticator authenticator, boolean async,
                               boolean emptyExchange) {
        def (clientMessages, serverResponses) = createMessages(payloads, emptyExchange)
        def connection = createConnection(serverResponses, emptyExchange ? -1 : 1)
        authenticate(connection, authenticator, async)
        validateClientMessages(connection, clientMessages, authenticator.getMechanismName())
    }

    def validateSpeculativeAuthentication(String payloads, ScramShaAuthenticator authenticator, boolean async,
                                          BsonDocument speculativeAuthenticateResponse) {
        def (clientMessages, serverResponses) = createMessages(payloads, false)
        def connection = createConnection(serverResponses, 0)
        def speculativeAuthenticateCommand = authenticator.createSpeculativeAuthenticateCommand(connection)
        authenticator.setSpeculativeAuthenticateResponse(speculativeAuthenticateResponse)

        authenticate(connection, authenticator, async)
        validateClientMessages(connection, clientMessages, authenticator.getMechanismName(), true)
        speculativeAuthenticateCommand
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

    def createMessages(String messages, boolean emptyExchange = true) {
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
        if (emptyExchange) {
            clientMessages += ''
        }
        [clientMessages, serverResponses]
    }
}
