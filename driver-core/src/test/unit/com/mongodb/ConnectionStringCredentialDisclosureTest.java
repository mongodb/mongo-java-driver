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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that no part of a connection string that may be a credential is disclosed by an error message, a log message or
 * {@link ConnectionString#toString()}.
 *
 * <p>Every connection string carries a sentinel, either as is or fully percent-encoded, in a position where the parser may
 * mistake a fragment of a password for something else, which is what happens when a password contains an unescaped {@code /} or
 * {@code ?}.</p>
 *
 * <p>See <a href="https://github.com/mongodb/specifications/blob/master/source/connection-string/tests/README.md#prose-tests">Prose Tests</a>.
 *
 * <p>NOTE: This class also contains tests that aren't categorized as Prose tests.</p>
 */
final class ConnectionStringCredentialDisclosureTest {

    private static final String SENTINEL = "SENTINELpassword1234";
    private static final String ENCODED_SENTINEL = SENTINEL.chars()
            .mapToObj(c -> String.format("%%%02X", c))
            .collect(Collectors.joining());
    private static final String USER = "reprouser";
    // The password head must be numeric: otherwise the relocated userinfo fails earlier as an invalid port
    private static final String HEAD = USER + ":12345";

    private static final Logger URI_LOGGER = (Logger) LoggerFactory.getLogger("org.mongodb.driver.uri");
    private static final ListAppender<ILoggingEvent> APPENDER = new ListAppender<>();

    @BeforeAll
    static void setUp() {
        APPENDER.setContext((LoggerContext) LoggerFactory.getILoggerFactory());
        URI_LOGGER.setLevel(Level.DEBUG);
        URI_LOGGER.addAppender(APPENDER);
        APPENDER.start();
    }

    @AfterAll
    static void cleanUp() {
        URI_LOGGER.detachAppender(APPENDER);
    }

    @AfterEach
    void reset() {
        APPENDER.list.clear();
    }

    static Stream<String> rejectedUris() {
        return Stream.of(
                // an option that is missing the value delimiter
                "mongodb://" + HEAD + "?" + SENTINEL + "@localhost:27017/admin",
                "mongodb://" + HEAD + "?" + ENCODED_SENTINEL,
                "mongodb://" + USER + "?" + SENTINEL + "@localhost:27017/admin",
                // integer valued options
                "mongodb://" + HEAD + "?connectTimeoutMS=" + SENTINEL + "@localhost:27017/admin",
                "mongodb://" + HEAD + "?connectTimeoutMS=" + ENCODED_SENTINEL,
                "mongodb://" + HEAD + "?maxPoolSize=" + SENTINEL,
                "mongodb://" + HEAD + "?srvMaxHosts=" + SENTINEL,
                "mongodb://" + HEAD + "?proxyHost=h&proxyPort=" + SENTINEL,
                "mongodb://" + HEAD + "?readPreference=secondary&maxStalenessSeconds=" + SENTINEL,
                "mongodb://" + HEAD + "?wtimeoutMS=" + SENTINEL + "@localhost:27017/admin",
                "mongodb://" + HEAD + "?zlibCompressionLevel=" + SENTINEL + "@localhost:27017/admin",
                // values from a restricted set
                "mongodb://" + HEAD + "?readPreference=" + SENTINEL + "@localhost:27017/admin",
                "mongodb://" + HEAD + "?readPreference=" + ENCODED_SENTINEL,
                "mongodb://" + HEAD + "?readConcernLevel=" + SENTINEL + "@localhost:27017/admin",
                "mongodb://" + HEAD + "?serverMonitoringMode=" + SENTINEL + "@localhost:27017/admin",
                "mongodb://" + HEAD + "?authMechanism=" + SENTINEL + "@localhost:27017/admin",
                "mongodb://" + HEAD + "?uuidRepresentation=" + SENTINEL + "@localhost:27017/admin",
                "mongodb://" + HEAD + "?compressors=" + SENTINEL,
                "mongodb://" + HEAD + "?compressors=" + ENCODED_SENTINEL,
                // read preference tags
                "mongodb://" + HEAD + "?readPreference=secondary&readPreferenceTags=" + SENTINEL,
                "mongodb://" + HEAD + "?readPreference=secondary&readPreferenceTags=dc:" + SENTINEL + ":x",
                // authentication mechanism properties
                "mongodb://" + USER + ":secret@localhost/?authMechanism=SCRAM-SHA-256&authMechanismProperties=" + SENTINEL,
                // duplicated proxy options
                "mongodb://" + HEAD + "?proxyHost=" + SENTINEL + "@localhost&proxyHost=b/admin",
                "mongodb://" + HEAD + "?proxyHost=h&proxyUsername=u&proxyPassword=" + SENTINEL + "&proxyPassword=" + SENTINEL + "x",
                "mongodb://" + HEAD + "?proxyHost=h&proxyUsername=" + SENTINEL + "&proxyUsername=u&proxyPassword=p",
                // hosts
                "mongodb://" + USER + ":" + SENTINEL + ":27017",
                "mongodb://" + USER + ":x:" + SENTINEL + "/x@localhost:27017/admin",
                "mongodb://" + HEAD + "," + SENTINEL + ",,x/x@localhost:27017/admin",
                "mongodb://" + HEAD + ",[" + SENTINEL + "/x@localhost:27017/admin",
                // URL decoding
                "mongodb://" + USER + ":" + SENTINEL + "%zz@localhost:27017",
                "mongodb://" + USER + "%" + SENTINEL + ":pw@localhost:27017",
                "mongodb://" + USER + ":pw@localhost:27017/" + SENTINEL + "%zz",
                "mongodb://" + HEAD + "?appName=" + SENTINEL + "%zz",
                // srvAllowedHostsSuffix
                "mongodb+srv://test12.test.build.10gen.cc/?srvAllowedHostsSuffix=" + SENTINEL + "*.10gen.cc"
        );
    }

    static Stream<String> acceptedUris() {
        return Stream.of(
                // unknown keys and boolean values are only logged
                "mongodb://" + HEAD + "?" + SENTINEL + "=1",
                "mongodb://" + HEAD + "?" + ENCODED_SENTINEL + "=1",
                "mongodb://" + HEAD + "?" + SENTINEL + "=1@localhost:27017/admin",
                "mongodb://" + HEAD + "?tls=" + SENTINEL,
                "mongodb://" + HEAD + "?tls=" + ENCODED_SENTINEL,
                "mongodb://" + HEAD + "?journal=" + SENTINEL + "&w=" + SENTINEL,
                "mongodb://" + HEAD + "?retryWrites=" + SENTINEL + "@localhost:27017/admin",
                "mongodb://" + HEAD + "?tlsInsecure=" + SENTINEL,
                "mongodb://" + USER + ":secret@localhost:27017/admin?" + SENTINEL + "%zz=1"
        );
    }

    @ParameterizedTest
    @MethodSource("rejectedUris")
    void shouldNotDiscloseSentinelWhenConnectionStringIsRejected(final String uri) {
        Exception e = assertThrows(IllegalArgumentException.class, () -> parse(uri));

        assertNoSentinel("exception message", e.getMessage());
        for (Throwable cause = e.getCause(); cause != null; cause = cause.getCause()) {
            assertNoSentinel("exception cause", String.valueOf(cause));
        }
        assertNoSentinelInLogs();
    }

    @ParameterizedTest
    @MethodSource("acceptedUris")
    void shouldNotDiscloseSentinelWhenConnectionStringIsAccepted(final String uri) {
        ConnectionString connectionString = assertDoesNotThrow(() -> parse(uri));

        assertNoSentinel("toString", connectionString.toString());
        assertNoSentinelInLogs();
    }

    @Test
    void shouldLogUnsupportedOptionWithoutItsKey() {
        new ConnectionString("mongodb://localhost/?" + SENTINEL + "=1&" + SENTINEL + "x=2");

        assertEquals(1, APPENDER.list.size());
        assertEquals("Connection string contains 2 unsupported option(s).", APPENDER.list.get(0).getFormattedMessage());
    }

    @Test
    void shouldLogDeprecatedBooleanValueWithoutTheValue() {
        new ConnectionString("mongodb://localhost/?journal=1");

        assertEquals(1, APPENDER.list.size());
        assertEquals("Deprecated boolean value in the connection string for 'journal'. Replace with 'true'",
                APPENDER.list.get(0).getFormattedMessage());
    }

    @Test
    void shouldReportTheNameOfTheOptionWithAnInvalidInteger() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> new ConnectionString("mongodb://localhost/?maxPoolSize=" + SENTINEL));

        assertEquals("The connection string contains an invalid value for 'maxpoolsize'. The value is not a valid integer",
                e.getMessage());
    }

    @Test
    void shouldRedactCredentialsInToString() {
        String uri = "mongodb://" + USER + ":" + SENTINEL + "@localhost:27017/db";

        ConnectionString connectionString = parse(uri);

        assertEquals("mongodb://<hidden>:<hidden>@localhost:27017/db", connectionString.toString());
        assertEquals(uri, connectionString.getConnectionString());
    }

    @Test
    void shouldRedactUserNameInToStringWhenThereIsNoPassword() {
        assertEquals("mongodb://<hidden>@localhost:27017/?authmechanism=<hidden>",
                new ConnectionString("mongodb://" + SENTINEL + "@localhost:27017/?authMechanism=MONGODB-X509").toString());
    }

    @Test
    void shouldRedactCredentialsInSrvToString() {
        // the host is resolved when the connection string is created, so a DNS client that returns no records is used
        ConnectionString connectionString = new ConnectionString("mongodb+srv://" + USER + ":" + SENTINEL + "@localhost.example.com/db",
                (name, type) -> Collections.emptyList());

        assertEquals("mongodb+srv://<hidden>:<hidden>@localhost.example.com/db", connectionString.toString());
    }

    @Test
    void shouldNotIncludeUserInformationInToStringWhenThereIsNone() {
        assertEquals("mongodb://host1:27017,host2:27018", new ConnectionString("mongodb://host2:27018,host1:27017").toString());
    }

    @Test
    void shouldRedactTheValuesOfAllOptionsAndOmitUnsupportedOptionsInToString() {
        ConnectionString connectionString = new ConnectionString("mongodb://localhost/db?maxPoolSize=10&proxyHost=h&proxyPort=1080"
                + "&proxyUsername=" + SENTINEL + "&proxyPassword=" + SENTINEL + "&" + SENTINEL + "=1&" + ENCODED_SENTINEL + "=2");

        String string = connectionString.toString();

        assertNoSentinel("toString", string);
        assertEquals("mongodb://localhost/db?maxpoolsize=<hidden>&proxyhost=<hidden>&proxypassword=<hidden>"
                + "&proxyport=<hidden>&proxyusername=<hidden>", string);
    }

    @Test
    void shouldRedactAuthMechanismPropertiesInToString() {
        ConnectionString connectionString = new ConnectionString("mongodb://localhost/?authMechanism=MONGODB-AWS"
                + "&authMechanismProperties=AWS_SESSION_TOKEN:" + SENTINEL);

        assertEquals("mongodb://localhost/?authmechanism=<hidden>&authmechanismproperties=<hidden>", connectionString.toString());
    }

    /**
     * <a href="https://github.com/mongodb/specifications/blob/master/source/connection-string/tests/README.md#prose-tests">
     * Test 1: Redacted string representation</a>.
     */
    @Test
    void shouldNotDiscloseCredentialsInAnyStringRepresentation() {
        String user = "SENTINELuser5678";
        ConnectionString connectionString = new ConnectionString("mongodb://" + user + ":" + SENTINEL + "@localhost:27017/db");
        MongoClientSettings settings = MongoClientSettings.builder().applyConnectionString(connectionString).build();

        for (Object object : new Object[] {connectionString, connectionString.getCredential(), settings}) {
            String string = object.toString();
            assertNoSentinel(object.getClass().getSimpleName(), string);
            assertFalse(string.contains(user), () -> object.getClass().getSimpleName() + " discloses the user name: " + string);
        }
        assertEquals("mongodb://<hidden>:<hidden>@localhost:27017/db", connectionString.toString());
        assertTrue(connectionString.getCredential().toString().contains("userName=<hidden>"));
        assertTrue(connectionString.getCredential().toString().contains("password=<hidden>"));
    }

    /**
     * <a href="https://github.com/mongodb/specifications/blob/master/source/connection-string/tests/README.md#prose-tests">
     * Test 2: Explicitly named unredacted API</a>.
     */
    @Test
    void shouldReturnTheOriginalConnectionStringFromGetConnectionString() {
        String uri = "mongodb://SENTINELuser5678:" + SENTINEL + "@localhost:27017/db";

        assertEquals(uri, parse(uri).getConnectionString());
    }

    /**
     * <a href="https://github.com/mongodb/specifications/blob/master/source/connection-string/tests/README.md#prose-tests">
     * Test 3: Canonical authentication mechanism diagnostic</a>.
     */
    @Test
    void shouldNotDiscloseTheSuppliedSpellingOfAnAuthenticationMechanism() {
        // a mixed-case spelling is not recognized, so it must not be echoed
        for (String spelling : new String[] {"mongoDB-X509", "mongodb-x509"}) {
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> new ConnectionString(
                    "mongodb://SENTINELuser5678:" + SENTINEL + "@localhost/?authMechanism=" + spelling));
            assertFalse(e.getMessage().contains(spelling), e.getMessage());
            assertNoSentinel("exception message", e.getMessage());
        }
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> new ConnectionString(
                "mongodb://SENTINELuser5678@localhost/?authMechanism=MONGODB-X509&authSource=admin"));
        assertEquals("Invalid authSource for MONGODB-X509, it must be '$external'", e.getMessage());
    }

    /**
     * <a href="https://github.com/mongodb/specifications/blob/master/source/connection-string/tests/README.md#prose-tests">
     * Test 4: Parsed integer diagnostic</a>.
     */
    @Test
    void shouldNotDiscloseIntegerArguments() {
        // no integer argument is reported, whether it fails conversion or a range check
        for (String uri : new String[] {
                "mongodb://localhost/?maxAdaptiveRetries=-0007",
                "mongodb://localhost/?maxAdaptiveRetries=%2D%30%30%30%37",
                "mongodb+srv://test12.test.build.10gen.cc/?srvMaxHosts=-0007"}) {
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> parse(uri));
            assertFalse(e.getMessage().contains("7"), e.getMessage());
        }
    }

    /**
     * <a href="https://github.com/mongodb/specifications/blob/master/source/connection-string/tests/README.md#prose-tests">
     * Test 5: Representation of a connection string that did not parse</a>.
     */
    @Test
    void shouldNotDiscloseCredentialsWhenConnectionStringDoesNotParse() {
        // the port is out of range
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> new ConnectionString(
                "mongodb://SENTINELuser5678:" + SENTINEL + "@localhost:99999/db"));

        assertNoSentinel("exception message", e.getMessage());
        assertFalse(e.getMessage().contains("SENTINELuser5678"), e.getMessage());
    }

    @Test
    void shouldDetectTheSentinel() {
        // positive control: the checks above are only meaningful if they fail when the sentinel is present
        assertTrue(containsSentinel("x " + SENTINEL + " x"));
        assertTrue(containsSentinel("x " + SENTINEL.toLowerCase(Locale.ROOT) + " x"));
        assertTrue(containsSentinel("x " + ENCODED_SENTINEL.toLowerCase(Locale.ROOT) + " x"));
        assertFalse(containsSentinel("<hidden>"));
        assertThrows(AssertionError.class, () -> assertNoSentinel("control", "leaked " + SENTINEL));
    }

    private static ConnectionString parse(final String uri) {
        // an SRV connection string resolves TXT records while it is parsed, so use a DNS client that returns no records
        // to avoid depending on the network
        return new ConnectionString(uri, (name, type) -> Collections.emptyList());
    }

    private static void assertNoSentinelInLogs() {
        List<ILoggingEvent> events = APPENDER.list;
        for (ILoggingEvent event : events) {
            assertNoSentinel("log message", event.getFormattedMessage());
        }
    }

    private static void assertNoSentinel(final String what, final String text) {
        assertFalse(containsSentinel(text), () -> what + " discloses the sentinel: " + text);
    }

    private static boolean containsSentinel(final String text) {
        String lowerCased = text.toLowerCase(Locale.ROOT);
        return lowerCased.contains(SENTINEL.toLowerCase(Locale.ROOT)) || lowerCased.contains(ENCODED_SENTINEL.toLowerCase(Locale.ROOT));
    }
}
