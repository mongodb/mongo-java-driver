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

import com.mongodb.Function;
import com.mongodb.MongoCommandException;
import com.mongodb.internal.ExceptionUtils.MongoCommandExceptionUtils;
import com.mongodb.internal.logging.LogMessage;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;

import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;

final class LogMatcher {
    private final ValueMatcher valueMatcher;
    private final AssertionContext context;

    LogMatcher(final ValueMatcher valueMatcher, final AssertionContext context) {

        this.valueMatcher = valueMatcher;
        this.context = context;
    }

    void assertLogMessageEquality(final String client, final BsonArray expectedMessages, final List<LogMessage> actualMessages,
            final Iterable<Tweak> tweaks) {
        context.push(ContextElement.ofLogMessages(client, expectedMessages, actualMessages));

        assertEquals(context.getMessage("Number of log messages must be the same"), expectedMessages.size(), actualMessages.size());

        for (int i = 0; i < expectedMessages.size(); i++) {
            BsonDocument expectedMessage = expectedMessages.get(i).asDocument().clone();
            for (Tweak tweak : tweaks) {
                expectedMessage = tweak.apply(expectedMessage);
            }
            if (expectedMessage != null) {
                valueMatcher.assertValuesMatch(expectedMessage, asDocument(actualMessages.get(i)));
            }
        }

        context.pop();
    }

     static BsonDocument asDocument(final LogMessage message) {
        BsonDocument document = new BsonDocument();
        document.put("component", new BsonString(message.getComponent().getValue()));
        document.put("level", new BsonString(message.getLevel().name().toLowerCase()));
        document.put("hasFailure", BsonBoolean.valueOf(message.getException() != null));
        document.put("failureIsRedacted",
                BsonBoolean.valueOf(message.getException() != null && exceptionIsRedacted(message.getException())));
        BsonDocument dataDocument = new BsonDocument();
        dataDocument.put("message", new BsonString(message.getMessageId()));
        if (message.getException() != null) {
            dataDocument.put("failure", new BsonString(message.getException().toString()));
        }

         Collection<LogMessage.Entry> entries = message.toStructuredLogMessage().getEntries();
         for (LogMessage.Entry entry : entries) {
            dataDocument.put(entry.getName(), asBsonValue(entry.getValue()));
        }
        document.put("data", dataDocument);

        return document;
    }

    private static boolean exceptionIsRedacted(final Throwable exception) {
        return exception instanceof MongoCommandException
                && MongoCommandExceptionUtils.SecurityInsensitiveResponseField.fieldNames()
                        .containsAll(((MongoCommandException) exception).getResponse().keySet());
    }

    private static BsonValue asBsonValue(final Object value) {
        if (value == null) {
            return BsonNull.VALUE;
        } else if (value instanceof String) {
            return new BsonString((String) value);
        } else if (value instanceof Integer) {
            return new BsonInt32((Integer) value);
        } else if (value instanceof Long) {
            return new BsonInt64((Long) value);
        } else if (value instanceof Double) {
            return new BsonDouble((Double) value);
        } else if (value instanceof Boolean) {
            return BsonBoolean.valueOf((Boolean) value);
        } else {
            return new BsonString(value.toString());
        }
    }

    interface Tweak extends Function<BsonDocument, BsonDocument> {
        /**
         * @param expectedMessage May be {@code null}, in which case the method simply returns {@code null}.
         * @return {@code null} iff matching {@code expectedMessage} with the actual message must be skipped.
         */
        @Nullable
        BsonDocument apply(@Nullable BsonDocument expectedMessage);

        static Tweak skip(final LogMessage.Entry.Name name) {
            return expectedMessage -> {
                if (expectedMessage == null) {
                    return null;
                } else {
                    BsonDocument expectedData = expectedMessage.getDocument("data", null);
                    if (expectedData != null) {
                        expectedData.remove(name.getValue());
                    }
                    return expectedMessage;
                }
            };
        }
    }
}
