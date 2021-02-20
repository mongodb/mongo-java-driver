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

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;

import static org.junit.Assert.assertNotNull;

public class UnifiedTestFailureValidator extends UnifiedTest {
    private Throwable exception;

    public UnifiedTestFailureValidator(@SuppressWarnings("unused") final String fileDescription,
                                       @SuppressWarnings("unused") final String testDescription,
                                       final String schemaVersion, @Nullable final BsonArray runOnRequirements, final BsonArray entities,
                                       final BsonArray initialData, final BsonDocument definition) {
        super(schemaVersion, runOnRequirements, entities, initialData, definition);
    }

    @Before
    public void setUp() {
        try {
            super.setUp();
        } catch (AssertionError | RuntimeException e) {
            exception = e;
        }
    }

    @Test
    public void shouldPassAllOutcomes() {
        if (exception == null) {
            try {
                super.shouldPassAllOutcomes();
            } catch (AssertionError | RuntimeException e) {
                exception = e;
            }
        }
        assertNotNull("Expected exception but not was thrown", exception);
    }

    @Override
    protected MongoClient createMongoClient(final MongoClientSettings settings) {
        return MongoClients.create(settings);
    }

    @Parameterized.Parameters(name = "{0}: {1}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        return getTestData("unified-test-format/valid-fail");
    }
}
