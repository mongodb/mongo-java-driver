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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Locale;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Case-insensitive matching of option names, hosts and keywords must not depend on the JVM default locale.
 * In the Turkish locale {@code "I".toLowerCase()} is the dotless {@code "ı"}.
 */
final class DefaultLocaleIndependenceTest {
    private Locale defaultLocale;

    @BeforeEach
    void setTurkishDefaultLocale() {
        defaultLocale = Locale.getDefault();
        Locale.setDefault(Locale.forLanguageTag("tr-TR"));
    }

    @AfterEach
    void restoreDefaultLocale() {
        Locale.setDefault(defaultLocale);
    }

    @Test
    void connectionStringOptionNames() {
        ConnectionString connectionString = new ConnectionString(
                "mongodb://db1.example.com/?maxIdleTimeMS=60000&tls=true&tlsAllowInvalidHostnames=true&readPreference=PRIMARY");
        assertEquals(60000, connectionString.getMaxConnectionIdleTime());
        assertEquals(true, connectionString.getSslInvalidHostnameAllowed());
        assertEquals(ReadPreference.primary(), connectionString.getReadPreference());
    }

    @Test
    void connectionStringAuthMechanismProperties() {
        MongoCredential gssapi = assertNotNull(new ConnectionString(
                "mongodb://foo:bar@example.com/?authMechanism=GSSAPI&authMechanismProperties=CANONICALIZE_HOST_NAME:true")
                .getCredential());
        assertEquals(true, gssapi.getMechanismProperty("CANONICALIZE_HOST_NAME", null));

        // the key contains an "I"
        MongoCredential oidc = assertNotNull(new ConnectionString(
                "mongodb://example.com/?authMechanism=MONGODB-OIDC&authMechanismProperties=ENVIRONMENT:test")
                .getCredential());
        assertEquals("test", oidc.getMechanismProperty(MongoCredential.ENVIRONMENT_KEY, null));
    }

    @Test
    void mechanismPropertyKeys() {
        MongoCredential credential = MongoCredential.createCredential("user", "db", "pwd".toCharArray())
                .withMechanismProperty("ENVIRONMENT", "test");
        assertEquals("test", credential.getMechanismProperty("environment", null));
        assertEquals("test", credential.getMechanismProperty("ENVIRONMENT", null));
    }

    @Test
    void serverAddressHost() {
        assertEquals("mongo-primary.local", new ServerAddress("MONGO-PRIMARY.LOCAL").getHost());
    }

    @Test
    void readPreferenceNames() {
        assertSame(ReadPreference.primary(), ReadPreference.valueOf("PRIMARY"));
        assertSame(ReadPreference.primaryPreferred(), ReadPreference.valueOf("PRIMARYPREFERRED"));
        assertEquals(ReadPreference.secondary().getName(), ReadPreference.valueOf("SECONDARY").getName());
    }

    @Test
    void writeConcernNames() {
        assertSame(WriteConcern.MAJORITY, WriteConcern.valueOf("MAJORITY"));
        assertSame(WriteConcern.JOURNALED, WriteConcern.valueOf("JOURNALED"));
    }

    @Test
    void compressorPropertyKeys() {
        MongoCompressor compressor = MongoCompressor.createZlibCompressor().withProperty("LEVEL", 3);
        assertEquals(3, compressor.getProperty("LEVEL", -1));
    }
}
