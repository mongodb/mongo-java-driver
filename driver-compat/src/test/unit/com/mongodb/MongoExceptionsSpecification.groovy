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

package com.mongodb

import org.mongodb.Document
import org.mongodb.MongoInterruptedException
import org.mongodb.command.MongoCommandFailureException
import org.mongodb.command.MongoDuplicateKeyException
import org.mongodb.connection.MongoSocketReadException
import org.mongodb.operation.MongoCursorNotFoundException
import org.mongodb.operation.ServerCursor
import spock.lang.Specification

import static com.mongodb.MongoExceptions.mapException

class MongoExceptionsSpecification extends Specification {

    def 'should convert InterruptedExceptions that are InterruptedIOExceptions into Network Exceptions'() {
        given:
        String expectedMessage = 'Interrupted IO Exception in the new architecture'
        def cause = new InterruptedIOException('The cause')

        when:
        MongoException actualException = mapException(new MongoInterruptedException(expectedMessage, cause))

        then:
        actualException instanceof MongoException.Network
        actualException.getCause() == cause
        actualException.getMessage() == expectedMessage
    }

    def 'should convert InterruptedExceptions that are not InterruptedIOExceptions into MongoException'() {
        given:
        String expectedMessage = 'Interrupted Exception in the new architecture'
        def cause = new InterruptedException('The cause')

        when:
        MongoException actualException = mapException(new MongoInterruptedException(expectedMessage, cause))

        then:
        !(actualException instanceof MongoException.Network)
        actualException instanceof MongoException
        actualException.getCause() == cause
        actualException.getMessage() == expectedMessage
    }

    def 'should convert IOExceptions into MongoException.Network'() {
        given:
        String expectedMessage = 'IO Exception in the new architecture'
        def cause = new IOException('The cause')

        when:
        MongoException actualException = mapException(new MongoSocketReadException(expectedMessage,
                                                                                   new org.mongodb.connection.ServerAddress(), cause))

        then:
        actualException instanceof MongoException.Network
        actualException.getCause() == cause
        actualException.getMessage() == expectedMessage
    }

    def 'should convert SocketExceptions that are not IOExceptions into MongoException'() {
        given:
        String expectedMessage = 'Exception in the new architecture'

        when:
        MongoException actualException = mapException(new MongoSocketReadException(expectedMessage,
                                                                                   new org.mongodb.connection.ServerAddress()))

        then:
        !(actualException instanceof MongoException.Network)
        actualException instanceof MongoException
        actualException.getMessage() == expectedMessage
    }

    def 'should convert MongoDuplicateKeyException into DuplicateKeyException'() {
        given:
        int expectedErrorCode = 500
        String expectedMessage = "Command failed with response { \"code\" : $expectedErrorCode } " +
                "on server ServerAddress{host='127.0.0.1', port=27017}"

        def newStyleException = new MongoDuplicateKeyException(
                new org.mongodb.operation.CommandResult(new Document(),
                                                        new org.mongodb.connection.ServerAddress(),
                                                        new Document
                                                        ('code', expectedErrorCode),
                                                        15L))
        when:
        MongoException actualException = mapException(newStyleException)

        then:
        actualException instanceof MongoException.DuplicateKey
        actualException.getMessage() == expectedMessage
        actualException.getCode() == expectedErrorCode
    }

    def 'should convert MongoCommandFailureException into CommandFailureException'() {
        given:
        int expectedErrorCode = 501
        String expectedMessage = "Command failed with response { \"code\" : $expectedErrorCode } " +
                "on server ServerAddress{host='127.0.0.1', port=27017}"

        def newStyleException = new MongoCommandFailureException(
                new org.mongodb.operation.CommandResult(new Document(),
                                                        new org.mongodb.connection.ServerAddress(),
                                                        new Document
                                                        ('code', expectedErrorCode),
                                                        15L))
        when:
        MongoException actualException = mapException(newStyleException)

        then:
        actualException instanceof CommandFailureException
        actualException.getMessage() == expectedMessage
        actualException.getCode() == expectedErrorCode
    }

    def 'should convert MongoCursorNotFoundException into MongoException.CursorNotFound'() {
        given:
        long cursorId = 123L
        org.mongodb.connection.ServerAddress serverAddress = new org.mongodb.connection.ServerAddress()
        String expectedMessage = "cursor $cursorId not found on server $serverAddress"

        when:
        MongoException actualException = mapException(new MongoCursorNotFoundException(new ServerCursor(cursorId, serverAddress)))

        then:
        actualException instanceof MongoException.CursorNotFound
        actualException.getMessage() == expectedMessage
        MongoException.CursorNotFound actualAsCursorNotFound = (MongoException.CursorNotFound) actualException
        actualAsCursorNotFound.getCursorId() == cursorId
        actualAsCursorNotFound.getServerAddress().getHost() == serverAddress.getHost()
        actualAsCursorNotFound.getServerAddress().getPort() == serverAddress.getPort()
    }

    def 'should not expose org.mongodb Exception in the stack trace of MongoInterruptedException'() {
        given:
        String expectedMessage = 'Interrupted IO Exception in the new architecture'
        def cause = new InterruptedIOException('The cause')

        when:
        MongoException actualException = mapException(new MongoInterruptedException(expectedMessage, cause))

        then:
        !(actualException.getCause() instanceof org.mongodb.MongoException)
        !actualException.getStackTrace().any { it.className.startsWith('org.mongodb') }
    }

    def 'should not expose org.mongodb Exception in the stack trace of MongoCursorNotFoundException'() {
        when:
        MongoException actualException = mapException(new MongoCursorNotFoundException(
                new ServerCursor(123L, new org.mongodb.connection.ServerAddress())))

        then:
        !(actualException.getCause() instanceof org.mongodb.MongoException)
        !actualException.getStackTrace().any { it.className.startsWith('org.mongodb') }
    }

    def 'should not expose org.mongodb Exception in the stack trace of MongoCommandFailureException'() {
        when:
        MongoException actualException = mapException(new MongoCommandFailureException(
                new org.mongodb.operation.CommandResult(new Document(),
                                                        new org.mongodb.connection.ServerAddress(),
                                                        new Document(),
                                                        15L)))

        then:
        !(actualException.getCause() instanceof org.mongodb.MongoException)
        !actualException.getStackTrace().any { it.className.startsWith('org.mongodb') }
    }

    def 'should not expose org.mongodb Exception in the stack trace of MongoDuplicateKeyException'() {
        when:

        MongoException actualException = mapException(new MongoDuplicateKeyException(
                new org.mongodb.operation.CommandResult(new Document(),
                                                        new org.mongodb.connection.ServerAddress(),
                                                        new Document(),
                                                        15L)))

        then:
        !(actualException.getCause() instanceof org.mongodb.MongoException)
        !actualException.getStackTrace().any { it.className.startsWith('org.mongodb') }
    }

}
