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
package com.mongodb.internal;

import com.mongodb.MongoCommandException;
import com.mongodb.ServerAddress;
import com.mongodb.internal.Exceptions.MongoCommandExceptions;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashSet;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ExceptionsTest {
    @Nested
    final class MongoCommandExceptionsTest {
        @Test
        void redacted() {
            MongoCommandException original = new MongoCommandException(
                    new BsonDocument("ok", BsonBoolean.FALSE)
                            .append("code", new BsonInt32(26))
                            .append("codeName", new BsonString("TimeoutError"))
                            .append("errorLabels", new BsonArray(asList(new BsonString("label"), new BsonString("label2"))))
                            .append("errmsg", new BsonString("err msg")),
                    new ServerAddress());
            MongoCommandException redacted = MongoCommandExceptions.redacted(original);
            assertArrayEquals(original.getStackTrace(), redacted.getStackTrace());
            String message = redacted.getMessage();
            assertTrue(message.contains("26"));
            assertTrue(message.contains("TimeoutError"));
            assertTrue(message.contains("label"));
            assertFalse(message.contains("err msg"));
            assertTrue(redacted.getErrorMessage().isEmpty());
            assertEquals(26, redacted.getErrorCode());
            assertEquals("TimeoutError", redacted.getErrorCodeName());
            assertEquals(new HashSet<>(asList("label", "label2")), redacted.getErrorLabels());
            assertEquals(MongoCommandExceptions.SecurityInsensitiveResponseField.fieldNames(), redacted.getResponse().keySet());
        }
    }
}
