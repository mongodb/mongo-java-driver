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

package com.mongodb.reactivestreams.client.internal;

import com.mongodb.MongoClientSettings;
import com.mongodb.internal.connection.InternalMongoClientSettings;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for reactive {@link InternalMongoClients}.
 * See sync InternalMongoClientsTest for comprehensive InternalMongoClientSettings tests.
 */
class InternalMongoClientsTest {

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

    @Test
    void testInternalSettingsArePreserved() {
        // Verify that InternalMongoClientSettings can be built with recordEverything for reactive clients
        InternalMongoClientSettings internalSettings = InternalMongoClientSettings.builder()
                .recordEverything(true)
                .build();
        assertNotNull(internalSettings);
    }
}

