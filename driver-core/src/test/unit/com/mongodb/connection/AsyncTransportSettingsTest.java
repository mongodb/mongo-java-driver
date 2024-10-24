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

package com.mongodb.connection;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class AsyncTransportSettingsTest {

    @Test
    public void shouldDefaultAllValuesToNull() {
        AsyncTransportSettings settings = TransportSettings.asyncBuilder().build();

        assertNull(settings.getExecutorService());
    }

    @Test
    public void shouldApplySettingsFromBuilder() {
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        AsyncTransportSettings settings = TransportSettings.asyncBuilder()
                .executorService(executorService)
                .build();

        assertEquals(executorService, settings.getExecutorService());
    }
}
