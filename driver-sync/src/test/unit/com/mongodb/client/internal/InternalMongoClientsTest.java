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

package com.mongodb.client.internal;

import com.mongodb.MongoClientSettings;
import com.mongodb.internal.connection.InternalConnectionPoolSettings;
import com.mongodb.internal.InternalMongoClientSettings;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link InternalMongoClients}.
 *
 * <p>These tests demonstrate how to use InternalMongoClients to create MongoClient instances
 * with custom internal settings, avoiding the need for global mutable state in tests.</p>
 */
class InternalMongoClientsTest {

    @Test
    void testCreateWithDefaultInternalSettings() {
        // Tests can now create clients with explicit internal settings
        InternalMongoClientSettings internalSettings = InternalMongoClientSettings.builder().build();

        assertNotNull(internalSettings.getInternalConnectionPoolSettings(),
                "Default internal settings should have connection pool settings");
        assertFalse(internalSettings.isRecordEverything(),
                "Default internal settings should have log recordEverything set to false");
    }

    @Test
    void testCreateWithCustomInternalConnectionPoolSettings() {
        // Demonstrates setting custom internal connection pool settings
        InternalConnectionPoolSettings poolSettings = InternalConnectionPoolSettings.builder()
                .prestartAsyncWorkManager(false)
                .build();

        InternalMongoClientSettings internalSettings = InternalMongoClientSettings.builder()
                .internalConnectionPoolSettings(poolSettings)
                .build();

        assertEquals(poolSettings, internalSettings.getInternalConnectionPoolSettings(),
                "Custom connection pool settings should be preserved");
    }

    @Test
    void testCreateWithCustomLogRecordingSettings() {
        InternalMongoClientSettings internalSettings = InternalMongoClientSettings.builder()
                .recordEverything(true)
                .build();

        assertTrue(internalSettings.isRecordEverything());
    }

    @Test
    void testGetDefaultsReturnsSameInstance() {
        InternalMongoClientSettings defaults1 = InternalMongoClientSettings.getDefaults();
        InternalMongoClientSettings defaults2 = InternalMongoClientSettings.getDefaults();
        assertSame(defaults1, defaults2, "getDefaults() should return the same instance");
    }

    @Test
    void testCreateMethodsValidateNullSettings() {
        // Verify that null MongoClientSettings is rejected
        assertThrows(IllegalArgumentException.class, () ->
                InternalMongoClients.create((MongoClientSettings) null, InternalMongoClientSettings.getDefaults()));

        // Verify that null InternalMongoClientSettings is rejected
        MongoClientSettings settings = MongoClientSettings.builder().build();
        assertThrows(IllegalArgumentException.class, () ->
                InternalMongoClients.create(settings, null));
    }
}
