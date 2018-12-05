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

package com.mongodb.embedded.client;

import org.junit.Test;
import org.junit.runner.RunWith;
import android.support.test.runner.AndroidJUnit4;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(AndroidJUnit4.class)
public class MongoEmbeddedSettingsTest {

    @Test
    public void shouldSetTheCorrectDefaults() {
        MongoEmbeddedSettings settings = MongoEmbeddedSettings.builder().build();

        assertNull(settings.getLibraryPath());
        assertNull(settings.getYamlConfig());
        assertEquals(MongoEmbeddedLogLevel.LOGGER, settings.getLogLevel());
    }

    @Test
    public void shouldSetTheCorrectSettings() {
        String libraryPath = "/mongo/lib/";
        MongoEmbeddedLogLevel logLevel = MongoEmbeddedLogLevel.NONE;
        String yamlConfig = "{systemLog: {verbosity: 5} }";

        MongoEmbeddedSettings settings = MongoEmbeddedSettings.builder()
                .libraryPath(libraryPath)
                .logLevel(logLevel)
                .yamlConfig(yamlConfig)
                .build();

        assertEquals(libraryPath, settings.getLibraryPath());
        assertEquals(logLevel, settings.getLogLevel());
        assertEquals(yamlConfig, settings.getYamlConfig());

        MongoEmbeddedSettings copiedSettings = MongoEmbeddedSettings.builder(settings).build();

        assertEquals(copiedSettings.getLibraryPath(), settings.getLibraryPath());
        assertEquals(copiedSettings.getLogLevel(), settings.getLogLevel());
        assertEquals(copiedSettings.getYamlConfig(), settings.getYamlConfig());
    }
}
