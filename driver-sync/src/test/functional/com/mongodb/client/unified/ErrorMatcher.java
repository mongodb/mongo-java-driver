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

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoClientException;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.MongoQueryException;
import com.mongodb.MongoServerException;
import com.mongodb.MongoSocketException;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import java.util.HashSet;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

final class ErrorMatcher {
    private static final Set<String> EXPECTED_ERROR_FIELDS = new HashSet<>(
            asList("isError", "expectError", "isClientError", "errorCode", "errorCodeName", "errorContains",
                    "isClientError", "errorLabelsOmit", "errorLabelsContain", "expectResult"));

    private final AssertionContext context;

    ErrorMatcher(final AssertionContext context) {
        this.context = context;
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
                            + " or IllegalStateException or MongoSocketException"),
                    expectedError.getBoolean("isClientError").getValue(),
                    e instanceof MongoClientException || e instanceof IllegalArgumentException || e instanceof IllegalStateException
                            || e instanceof MongoSocketException);
        }
        if (expectedError.containsKey("errorContains")) {
            String errorContains = expectedError.getString("errorContains").getValue();
            assertTrue(context.getMessage("Error message does not contain expected string: " + errorContains),
                    e.getMessage().contains(errorContains));
        }
        if (expectedError.containsKey("errorCode")) {
            assertTrue(context.getMessage("Exception must be of type MongoCommandException or MongoQueryException when checking"
                            + " for error codes"),
                    e instanceof MongoCommandException || e instanceof MongoQueryException);
            int errorCode = (e instanceof MongoCommandException)
                    ? ((MongoCommandException) e).getErrorCode()
                    : ((MongoQueryException) e).getErrorCode();

            assertEquals(context.getMessage("Error codes must match"), expectedError.getNumber("errorCode").intValue(),
                    errorCode);
        }
        if (expectedError.containsKey("errorCodeName")) {
            assertTrue(context.getMessage("Exception must be of type MongoServerException when checking for error codes"),
                    e instanceof MongoServerException);
            MongoServerException mongoServerException = (MongoServerException) e;
            assertEquals(context.getMessage("Error code names must match"), expectedError.getString("errorCodeName").getValue(),
                    mongoServerException.getErrorCodeName());
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
        if (expectedError.containsKey("expectResult")) {
            // MongoBulkWriteException does not include information about the successful writes, so this is the only check
            // that can currently be done
            assertTrue(context.getMessage("Exception must be of type MongoBulkWriteException when checking for results"),
                    e instanceof MongoBulkWriteException);
        }
        context.pop();
    }
}
