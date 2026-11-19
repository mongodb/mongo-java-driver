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

package com.mongodb;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.bson.BsonDocument;
import org.junit.Test;
import org.junit.runners.Parameterized;
import org.slf4j.LoggerFactory;
import util.JsonPoweredTestHelper;

import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.junit.Assume.assumeFalse;

// See https://github.com/mongodb/specifications/tree/master/source/connection-string/tests
public class ConnectionStringTest extends AbstractConnectionStringTest {
    public ConnectionStringTest(final String filename, final String description, final String input, final BsonDocument definition) {
        super(filename, description, input, definition);
    }

    @Test
    public void shouldPassAllOutcomes() {
        // Java driver currently throws an IllegalArgumentException for these tests
        assumeFalse(getDescription().equals("Empty integer option values are ignored"));
        assumeFalse(getDescription().equals("Comma in a key value pair causes a warning"));

        if (getFilename().equals("invalid-uris.json")) {
            testInvalidUris();
        } else if (getFilename().equals("valid-auth.json")) {
            testValidAuth();
        } else if (getFilename().equals("valid-db-with-dotted-name.json")) {
            testValidHostIdentifiers();
            testValidAuth();
        } else if (getFilename().equals("valid-host_identifiers.json")) {
            testValidHostIdentifiers();
        } else if (getFilename().equals("valid-options.json")) {
            testValidOptions();
        } else if (getFilename().equals("valid-unix_socket-absolute.json")) {
            testValidHostIdentifiers();
        } else if (getFilename().equals("valid-unix_socket-relative.json")) {
            testValidHostIdentifiers();
        } else if (getFilename().equals("valid-warnings.json")) {
            testValidHostIdentifiers();
            if (!getDefinition().get("options").isNull()) {
                testValidOptions();
            }
        } else if (getFilename().equals("credential-disclosure.json")) {
            testCredentialDisclosure();
        } else {
            throw new IllegalArgumentException("Unsupported file: " + getFilename());
        }
    }

    // The sentinel credential must not appear, in plain or fully percent-encoded form, in the error message or in any message logged
    // while parsing, whether or not the URI is accepted. See credential-disclosure.json in the connection string specification tests.
    private void testCredentialDisclosure() {
        String uri = getDefinition().getString("uri").getValue();
        Logger logger = (Logger) LoggerFactory.getLogger("org.mongodb.driver.uri");
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.setContext((LoggerContext) LoggerFactory.getILoggerFactory());
        logger.setLevel(Level.DEBUG);
        logger.addAppender(appender);
        appender.start();
        try {
            try {
                // an SRV connection string resolves TXT records while it is parsed, so use a DNS client that returns no records
                new ConnectionString(uri, (name, type) -> Collections.emptyList());
            } catch (Throwable t) {
                assertFalse("The error message discloses the sentinel: " + t.getMessage(), containsSentinel(String.valueOf(t.getMessage())));
                for (Throwable cause = t.getCause(); cause != null; cause = cause.getCause()) {
                    assertFalse("The error cause discloses the sentinel: " + cause, containsSentinel(String.valueOf(cause)));
                }
            }
            for (ILoggingEvent event : appender.list) {
                assertFalse("A log message discloses the sentinel: " + event.getFormattedMessage(),
                        containsSentinel(event.getFormattedMessage()));
            }
        } finally {
            logger.detachAppender(appender);
        }
    }

    private static boolean containsSentinel(final String text) {
        String sentinel = "SENTINELpassword1234";
        String encodedSentinel = sentinel.chars().mapToObj(c -> String.format("%%%02X", c)).collect(Collectors.joining());
        String lowerCased = text.toLowerCase(Locale.ROOT);
        return lowerCased.contains(sentinel.toLowerCase(Locale.ROOT)) || lowerCased.contains(encodedSentinel.toLowerCase(Locale.ROOT));
    }

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() {
        return JsonPoweredTestHelper.getTestData("connection-string");
    }
}
