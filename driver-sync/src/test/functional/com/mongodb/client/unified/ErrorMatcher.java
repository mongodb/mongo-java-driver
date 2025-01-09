/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.client.unified;

import com.mongodb.ClientBulkWriteException;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoClientException;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.MongoSecurityException;
import com.mongodb.MongoExecutionTimeoutException;
import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.MongoServerException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.MongoWriteException;
import com.mongodb.WriteError;
import com.mongodb.bulk.WriteConcernError;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static java.lang.Integer.parseInt;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

final class ErrorMatcher {
    private static final Set<String> EXPECTED_ERROR_FIELDS = new HashSet<>(
            asList("isError", "expectError", "isClientError", "errorCode", "errorCodeName", "errorContains", "errorResponse",
                    "isClientError", "isTimeoutError", "errorLabelsOmit", "errorLabelsContain",
                    "writeErrors", "writeConcernErrors", "expectResult"));

    private final AssertionContext context;
    private final ValueMatcher valueMatcher;

    ErrorMatcher(final AssertionContext context, final ValueMatcher valueMatcher) {
        this.context = context;
        this.valueMatcher = valueMatcher;
    }

    void assertErrorsMatch(final BsonDocument expectedError, final Exception e) {
        context.push(ContextElement.ofError(expectedError, e));

        assertTrue(context.getMessage("Unexpected field in expectError.  One of " + expectedError.keySet()),
                EXPECTED_ERROR_FIELDS.containsAll(expectedError.keySet()));

        if (expectedError.containsKey("isError")) {
            assertTrue(context.getMessage("isError must be true"), expectedError.getBoolean("isError").getValue());
        }
        if (expectedError.containsKey("isClientError")) {
            assertEquals(context.getMessage("Exception must be of type MongoClientException or IllegalArgumentException"
                            + " or IllegalStateException or MongoSocketException or MongoInternalException"),
                    expectedError.getBoolean("isClientError").getValue(),
                    e instanceof MongoClientException || e instanceof IllegalArgumentException || e instanceof IllegalStateException
                            || e instanceof MongoSocketException);
        }

        if (expectedError.containsKey("isTimeoutError")) {
            assertEquals(context.getMessage("Exception must be of type MongoOperationTimeoutException when checking for results"),
                    expectedError.getBoolean("isTimeoutError").getValue(),
                    e instanceof MongoOperationTimeoutException
            );
        }

        if (expectedError.containsKey("errorContains")) {
            String errorContains = expectedError.getString("errorContains").getValue();
            assertTrue(context.getMessage("Error message does not contain expected string: " + errorContains),
                    e.getMessage().toLowerCase(Locale.ROOT).contains(errorContains.toLowerCase(Locale.ROOT)));
        }
        if (expectedError.containsKey("errorResponse")) {
            valueMatcher.assertValuesMatch(expectedError.getDocument("errorResponse"), ((MongoCommandException) e).getResponse());
        }
        if (expectedError.containsKey("errorCode")) {
            Exception errorCodeException = e;
            if (e instanceof MongoSecurityException && e.getCause() instanceof MongoCommandException) {
                errorCodeException = (Exception) e.getCause();
            }
            assertTrue(context.getMessage("Exception must be of type MongoCommandException or MongoWriteException when checking"
                            + " for error codes, but was " + e.getClass().getSimpleName()),
                    errorCodeException instanceof MongoCommandException
                            || errorCodeException instanceof MongoWriteException);
            int errorCode = (errorCodeException instanceof MongoCommandException)
                    ? ((MongoCommandException) errorCodeException).getErrorCode()
                    : ((MongoWriteException) errorCodeException).getCode();

            assertEquals(context.getMessage("Error codes must match"), expectedError.getNumber("errorCode").intValue(),
                    errorCode);
        }
        if (expectedError.containsKey("errorCodeName")) {
            String expectedErrorCodeName = expectedError.getString("errorCodeName").getValue();
            if (e instanceof MongoExecutionTimeoutException) {
                assertEquals(context.getMessage("Error code names must match"), expectedErrorCodeName, "MaxTimeMSExpired");
            } else if (e instanceof MongoWriteConcernException) {
                assertEquals(context.getMessage("Error code names must match"), expectedErrorCodeName,
                        ((MongoWriteConcernException) e).getWriteConcernError().getCodeName());
            } else if (e instanceof MongoServerException) {
                assertEquals(context.getMessage("Error code names must match"), expectedErrorCodeName,
                        ((MongoServerException) e).getErrorCodeName());
            } else {
                fail(context.getMessage(String.format("Unexpected exception type %s when asserting error code name",
                        e.getClass().getSimpleName())));
            }
        }
        if (expectedError.containsKey("errorLabelsOmit")) {
            assertTrue(context.getMessage("Exception must be of type MongoException when checking for error labels"),
                    e instanceof MongoException);
            MongoException mongoException = (MongoException) e;
            for (BsonValue cur : expectedError.getArray("errorLabelsOmit")) {
                assertFalse(context.getMessage("Expected error label to be omitted but it is not: " + cur.asString().getValue()),
                        mongoException.hasErrorLabel(cur.asString().getValue()));
            }
        }
        if (expectedError.containsKey("errorLabelsContain")) {
            assertTrue(context.getMessage("Exception must be of type MongoException when checking for error labels"),
                    e instanceof MongoException);
            MongoException mongoException = (MongoException) e;
            for (BsonValue cur : expectedError.getArray("errorLabelsContain")) {
                assertTrue(context.getMessage("Expected error label: " + cur.asString().getValue()),
                        mongoException.hasErrorLabel(cur.asString().getValue()));
            }
        }
        if (expectedError.containsKey("writeErrors")) {
            assertTrue(context.getMessage("Exception must be of type ClientBulkWriteException when checking for write errors"),
                    e instanceof ClientBulkWriteException);
            BsonDocument writeErrors = expectedError.getDocument("writeErrors");
            ClientBulkWriteException actualException = (ClientBulkWriteException) e;
            Map<Integer, WriteError> actualWriteErrors = actualException.getWriteErrors();
            assertEquals("The number of write errors must match", writeErrors.size(), actualWriteErrors.size());
            writeErrors.forEach((index, writeError) -> {
                WriteError actualWriteError = actualWriteErrors.get(parseInt(index));
                assertNotNull("Expected a write error with index " + index, actualWriteError);
                valueMatcher.assertValuesMatch(writeError, toMatchableValue(actualWriteError));
            });
        }
        if (expectedError.containsKey("writeConcernErrors")) {
            assertTrue(context.getMessage("Exception must be of type ClientBulkWriteException when checking for write errors"),
                    e instanceof ClientBulkWriteException);
            List<BsonDocument> writeConcernErrors = expectedError.getArray("writeConcernErrors").stream()
                    .map(BsonValue::asDocument).collect(toList());
            ClientBulkWriteException actualException = (ClientBulkWriteException) e;
            List<WriteConcernError> actualWriteConcernErrors = actualException.getWriteConcernErrors();
            assertEquals("The number of write concern errors must match", writeConcernErrors.size(), actualWriteConcernErrors.size());
            for (int index = 0; index < writeConcernErrors.size(); index++) {
                BsonDocument writeConcernError = writeConcernErrors.get(index);
                WriteConcernError actualWriteConcernError = actualWriteConcernErrors.get(index);
                valueMatcher.assertValuesMatch(writeConcernError, toMatchableValue(actualWriteConcernError));
            }
        }
        if (expectedError.containsKey("expectResult")) {
            assertTrue(context.getMessage("Exception must be of type"
                            + " MongoBulkWriteException, or MongoSocketException, or ClientBulkWriteException"
                            + " when checking for results, but actual type is " + e.getClass().getSimpleName()),
                    e instanceof MongoBulkWriteException || e instanceof ClientBulkWriteException || e instanceof MongoSocketException);
            // neither `MongoBulkWriteException` nor `MongoSocketException` includes information about the successful individual operations
            if (e instanceof ClientBulkWriteException) {
                BsonDocument actualPartialResult = ((ClientBulkWriteException) e).getPartialResult()
                        .map(UnifiedCrudHelper::toMatchableValue)
                        .orElse(new BsonDocument());
                valueMatcher.assertValuesMatch(expectedError.getDocument("expectResult"), actualPartialResult);
            }
        }
        context.pop();
    }

    private static BsonDocument toMatchableValue(final WriteError writeError) {
        return new BsonDocument("code", new BsonInt32(writeError.getCode()));
    }

    private static BsonDocument toMatchableValue(final WriteConcernError writeConcernError) {
        return new BsonDocument("code", new BsonInt32(writeConcernError.getCode()))
                .append("message", new BsonString(writeConcernError.getMessage()));
    }
}
