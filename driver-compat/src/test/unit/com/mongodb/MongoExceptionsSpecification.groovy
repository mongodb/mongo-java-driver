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
import org.mongodb.command.MongoWriteConcernException
import org.mongodb.connection.MongoSocketReadException
import org.mongodb.connection.MongoTimeoutException
import org.mongodb.connection.MongoWaitQueueFullException
import org.mongodb.operation.MongoCursorNotFoundException
import org.mongodb.operation.ServerCursor
import spock.lang.Specification

import static com.mongodb.MongoExceptions.mapException

class MongoExceptionsSpecification extends Specification {

    def 'should convert InterruptedExceptions that are not InterruptedIOExceptions into MongoInterruptedException'() {
        given:
        String expectedMessage = 'Interrupted Exception in the new architecture'
        def cause = new InterruptedException('The cause')

        when:
        MongoException actualException = mapException(new MongoInterruptedException(expectedMessage, cause))

        then:
        actualException instanceof com.mongodb.MongoInterruptedException
        actualException.getCause() == cause
        actualException.getMessage() == expectedMessage
        assertExceptionFromNewArchitectureIsNotVisibleOnStackTrace(actualException)
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
        assertExceptionFromNewArchitectureIsNotVisibleOnStackTrace(actualException)
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
        assertExceptionFromNewArchitectureIsNotVisibleOnStackTrace(actualException)
    }

    def 'should convert MongoDuplicateKeyException into DuplicateKeyException'() {
        given:
        int expectedErrorCode = 500
        String expectedMessage = "Command failed with response { \"code\" : $expectedErrorCode } " +
                "on server ServerAddress{host='127.0.0.1', port=27017}"

        when:
        MongoException actualException = mapException(new MongoDuplicateKeyException(commandResultWithErrorCode(expectedErrorCode)))

        then:
        actualException instanceof MongoException.DuplicateKey
        actualException.getMessage() == expectedMessage
        actualException.getCode() == expectedErrorCode
        assertExceptionFromNewArchitectureIsNotVisibleOnStackTrace(actualException)
    }

    def 'should convert MongoCommandFailureException into CommandFailureException'() {
        given:
        int expectedErrorCode = 501
        String expectedMessage = "Command failed with response { \"code\" : $expectedErrorCode } " +
                "on server ServerAddress{host='127.0.0.1', port=27017}"

        def newStyleException = new MongoCommandFailureException(commandResultWithErrorCode(expectedErrorCode))
        when:
        MongoException actualException = mapException(newStyleException)

        then:
        actualException instanceof CommandFailureException
        actualException.getMessage() == expectedMessage
        actualException.getCode() == expectedErrorCode
        assertExceptionFromNewArchitectureIsNotVisibleOnStackTrace(actualException)
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

        assertExceptionFromNewArchitectureIsNotVisibleOnStackTrace(actualException)
    }

    def 'should convert org.mongodb.MongoInternalException into com.mongodb.MongoInternalException'() {
        given:
        String expectedMessage = 'Internal Exception thrown'

        when:
        MongoException actualException = mapException(new org.mongodb.MongoInternalException(expectedMessage))

        then:
        actualException instanceof MongoInternalException
        actualException.getMessage() == expectedMessage
        assertExceptionFromNewArchitectureIsNotVisibleOnStackTrace(actualException)
    }

    def 'should convert MongoWriteConcernException into com.mongodb.WriteConcernException'() {
        given:
        int expectedErrorCode = 500
        String expectedMessage = "Command failed with response { \"code\" : $expectedErrorCode } " +
                "on server ServerAddress{host='127.0.0.1', port=27017}"

        when:
        MongoException actualException = mapException(new MongoWriteConcernException(commandResultWithErrorCode(expectedErrorCode)))

        then:
        actualException instanceof WriteConcernException
        actualException.getMessage() == expectedMessage
        actualException.getCode() == expectedErrorCode
        assertExceptionFromNewArchitectureIsNotVisibleOnStackTrace(actualException)
    }

    def 'should convert MongoTimeoutException into com.mongodb.ConnectionWaitTimeOut'() {
        given:
        String expectedMessage = 'A timeout exception was throwm'

        when:
        MongoException actualException = mapException(new MongoTimeoutException(expectedMessage))

        then:
        actualException instanceof ConnectionWaitTimeOut
        actualException.getMessage() == expectedMessage
        assertExceptionFromNewArchitectureIsNotVisibleOnStackTrace(actualException)
    }

    def 'should convert MongoWaitQueueFullException into com.mongodb.SemaphoresOut'() {
        given:
        String expectedMessage = 'A queue full exception was throwm'

        when:
        MongoException actualException = mapException(new MongoWaitQueueFullException(expectedMessage))

        then:
        actualException instanceof SemaphoresOut
        actualException.getMessage() == expectedMessage
        assertExceptionFromNewArchitectureIsNotVisibleOnStackTrace(actualException)
    }

    private static org.mongodb.operation.CommandResult commandResultWithErrorCode(int expectedErrorCode) {
        new org.mongodb.operation.CommandResult(new Document(),
                                                new org.mongodb.connection.ServerAddress(),
                                                new Document
                                                ('code', expectedErrorCode),
                                                15L)
    }

    private static void assertExceptionFromNewArchitectureIsNotVisibleOnStackTrace(MongoException actualException) {
        assert !(actualException.getCause() instanceof org.mongodb.MongoException)
        assert !actualException.getStackTrace().any { it.className.startsWith('org.mongodb') }
    }

}
