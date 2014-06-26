/*
 * Copyright (c) 2008 - 2014 MongoDB, Inc.
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
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.mongodb.MongoCommandFailureException
import org.mongodb.MongoWriteException
import org.mongodb.connection.ServerAddress
import spock.lang.Specification
import spock.lang.Unroll

import static com.mongodb.MongoExceptions.mapException

@SuppressWarnings('LineLength')
class MongoExceptionsSpecification extends Specification {
    private final static String MESSAGE = 'New style exception'
    private final static int ERROR_CODE = 500

    @Unroll
    def 'should convert #exceptionToBeMapped into #exceptionForCompatibilityApi'() {

        expect:
        MongoException actualException = mapException(exceptionToBeMapped)
        actualException.class == exceptionForCompatibilityApi
        actualException.getCause() == exceptionToBeMapped.getCause()
        actualException.getCode() == errorCode
        actualException.getMessage() == exceptionToBeMapped.getMessage()
        !(actualException.getCause() instanceof org.mongodb.MongoException)
        !actualException.getStackTrace().any { it.className.startsWith('org.mongodb') }

        where:
        exceptionToBeMapped                                                                   | exceptionForCompatibilityApi     | errorCode
        new MongoCommandFailureException(commandResultWithErrorCode(ERROR_CODE))              | CommandFailureException          |
        ERROR_CODE
        new MongoWriteException(ERROR_CODE, MESSAGE, commandResultWithErrorCode(ERROR_CODE))  | WriteConcernException            |
        ERROR_CODE
    }

    private static org.mongodb.CommandResult commandResultWithErrorCode(int expectedErrorCode) {
        new org.mongodb.CommandResult(new ServerAddress(), new BsonDocument('code', new BsonInt32(expectedErrorCode)), 15L)
    }

}
