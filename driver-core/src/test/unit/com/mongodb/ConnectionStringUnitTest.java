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

import com.mongodb.assertions.Assertions;
import com.mongodb.connection.ServerMonitoringMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class ConnectionStringUnitTest {
    private static final String DEFAULT_OPTIONS = "mongodb://localhost/?";
    @Test
    void defaults() {
        ConnectionString connectionStringDefault = new ConnectionString(DEFAULT_OPTIONS);
        assertAll(() -> assertNull(connectionStringDefault.getServerMonitoringMode()));
    }

    @Test
    public void mustDecodeNonOidcAsWhole()  {
        // this string allows us to check if there is no double decoding
        String rawValue = encode("ot her");
        assertAll(() -> {
            // even though only one part has been encoded by the user, the whole option value (pre-split) must be decoded
            ConnectionString cs = new ConnectionString(
                    "mongodb://foo:bar@example.com/?authMechanism=GSSAPI&authMechanismProperties="
                            + "SERVICE_NAME:" + encode(rawValue) + ",CANONICALIZE_HOST_NAME:true&authSource=$external");
            MongoCredential credential = Assertions.assertNotNull(cs.getCredential());
            assertEquals(rawValue, credential.getMechanismProperty("SERVICE_NAME", null));
        }, () -> {
            ConnectionString cs = new ConnectionString(
                    "mongodb://foo:bar@example.com/?authMechanism=GSSAPI&authMechanismProperties="
                            + encode("SERVICE_NAME:" + rawValue + ",CANONICALIZE_HOST_NAME:true&authSource=$external"));
            MongoCredential credential = Assertions.assertNotNull(cs.getCredential());
            assertEquals(rawValue, credential.getMechanismProperty("SERVICE_NAME", null));
        });
    }

    private static String encode(final String string) {
        try {
            return URLEncoder.encode(string, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {DEFAULT_OPTIONS + "serverMonitoringMode=stream"})
    void equalAndHashCode(final String connectionString) {
        ConnectionString default1 = new ConnectionString(DEFAULT_OPTIONS);
        ConnectionString default2 = new ConnectionString(DEFAULT_OPTIONS);
        ConnectionString actual1 = new ConnectionString(connectionString);
        ConnectionString actual2 = new ConnectionString(connectionString);
        assertAll(
                () -> assertEquals(default1, default2),
                () -> assertEquals(default1.hashCode(), default2.hashCode()),
                () -> assertEquals(actual1, actual2),
                () -> assertEquals(actual1.hashCode(), actual2.hashCode()),
                () -> assertNotEquals(default1, actual1)
        );
    }

    @Test
    void serverMonitoringMode() {
        assertAll(
                () -> assertEquals(ServerMonitoringMode.POLL,
                        new ConnectionString(DEFAULT_OPTIONS + "serverMonitoringMode=poll").getServerMonitoringMode()),
                () -> assertThrows(IllegalArgumentException.class,
                        () -> new ConnectionString(DEFAULT_OPTIONS + "serverMonitoringMode=invalid"))
        );
    }


    @ParameterizedTest
    @ValueSource(strings = {"mongodb://foo:bar/@hostname/java?", "mongodb://foo:bar?@hostname/java/",
                            "mongodb+srv://foo:bar/@hostname/java?", "mongodb+srv://foo:bar?@hostname/java/",
                            "mongodb://foo:bar/@[::1]:27018", "mongodb://foo:bar?@[::1]:27018",
                            "mongodb://foo:12345678/@hostname", "mongodb+srv://foo:12345678/@hostname",
                            "mongodb://foo:12345678/@hostname", "mongodb+srv://foo:12345678/@hostname",
                            "mongodb://foo:12345678%40hostname", "mongodb+srv://foo:12345678%40hostname",
                            "mongodb://foo:12345678@bar@hostname", "mongodb+srv://foo:12345678@bar@hostname"
    })
    void unescapedPasswordsShouldNotBeLeakedInExceptionMessages(final String input) {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> new ConnectionString(input));
        assertFalse(exception.getMessage().contains("bar"));
        assertFalse(exception.getMessage().contains("12345678"));
    }
}
