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

import com.mongodb.MongoCommandException;
import com.mongodb.internal.logging.StructuredLogMessage;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;

import java.util.List;

import static org.junit.Assert.assertEquals;

final class LogMatcher {
    private final ValueMatcher valueMatcher;
    private final AssertionContext context;

    LogMatcher(final ValueMatcher valueMatcher, final AssertionContext context) {

        this.valueMatcher = valueMatcher;
        this.context = context;
    }

    void assertLogMessageEquality(final String client, final BsonArray expectedMessages, final List<StructuredLogMessage> actualMessages) {
        context.push(ContextElement.ofLogMessages(client, expectedMessages, actualMessages));

        assertEquals(context.getMessage("Number of log messages must be the same"), expectedMessages.size(), actualMessages.size());

        for (int i = 0; i < expectedMessages.size(); i++) {
            BsonDocument expectedMessageAsDocument = expectedMessages.get(i).asDocument().clone();
            expectedMessageAsDocument.remove("failureIsRedacted");
            valueMatcher.assertValuesMatch(expectedMessageAsDocument, asDocument(actualMessages.get(i)));
        }

        context.pop();
    }

    private static BsonDocument asDocument(final StructuredLogMessage message) {
        BsonDocument document = new BsonDocument();
        document.put("component", new BsonString(message.getLoggerName().substring(message.getLoggerName().lastIndexOf(".") + 1)));
        document.put("level", new BsonString(message.getLevel()));
        document.put("hasFailure", BsonBoolean.valueOf(message.getException() != null && exceptionIsUnredacted(message.getException())));

        BsonDocument dataDocument = new BsonDocument();
        dataDocument.put("message", new BsonString(message.getMessageId()));
        if (message.getException() != null) {
            dataDocument.put("failure", new BsonString(message.getException().toString()));
        }
        for (StructuredLogMessage.Entry entry : message.getEntries()) {
            dataDocument.put(entry.getName(), asBsonValue(entry.getValue()));
        }
        document.put("data", dataDocument);

        return document;
    }

    private static boolean exceptionIsUnredacted(final Throwable exception) {
        return exception instanceof MongoCommandException && !((MongoCommandException) exception).getResponse().isEmpty();
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

}
