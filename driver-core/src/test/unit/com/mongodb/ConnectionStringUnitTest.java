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

import com.mongodb.connection.ServerMonitoringMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
    public void mustDecodeOidcIndividually() {
        String string = "abc,d!@#$%^&*;ef:ghi";
        ConnectionString cs = new ConnectionString(
                "mongodb://localhost/?authMechanism=MONGODB-OIDC&authMechanismProperties="
                        + "ENVIRONMENT:azure,TOKEN_RESOURCE:" + encode(string));
        assertEquals(string, cs.getCredential().getMechanismProperty("TOKEN_RESOURCE", null));
    }

    @Test
    public void mustDecodeNonOidcAsWhole()  {
        ConnectionString cs2 = new ConnectionString(
                "mongodb://foo:bar@example.com/?authMechanism=GSSAPI&authMechanismProperties="
                        + encode("SERVICE_NAME:other,CANONICALIZE_HOST_NAME:true&authSource=$external"));
        assertEquals("other", cs2.getCredential().getMechanismProperty("SERVICE_NAME", null));
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
}
