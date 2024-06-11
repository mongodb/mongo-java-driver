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

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * {@link SocketSettingsSpecification} contains older unit tests for {@link SocketSettings}.
 */
final class SocketSettingsTest {
    @Test
    void connectTimeoutThrowsIfArgumentIsTooLarge() {
        assertThrows(IllegalArgumentException.class, () -> SocketSettings.builder().connectTimeout(Integer.MAX_VALUE / 2, TimeUnit.SECONDS));
    }

    @Test
    void readTimeoutThrowsIfArgumentIsTooLarge() {
        assertThrows(IllegalArgumentException.class, () -> SocketSettings.builder().readTimeout(Integer.MAX_VALUE / 2, TimeUnit.SECONDS));
    }
}
