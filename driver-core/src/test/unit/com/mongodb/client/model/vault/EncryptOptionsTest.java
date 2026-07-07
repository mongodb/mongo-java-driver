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

package com.mongodb.client.model.vault;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EncryptOptionsTest {

    @Test
    void shouldStoreStringOptions() {
        StringOptions stringOptions = new StringOptions().caseSensitive(true);
        EncryptOptions options = new EncryptOptions("String").stringOptions(stringOptions);
        assertSame(stringOptions, options.getStringOptions());
        assertNull(options.getTextOptions());
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldStoreDeprecatedTextOptionsIndependently() {
        TextOptions textOptions = new TextOptions().caseSensitive(true);
        EncryptOptions options = new EncryptOptions("String").textOptions(textOptions);
        assertSame(textOptions, options.getTextOptions());
        assertNull(options.getStringOptions());
    }

    @Test
    void toStringShouldIncludeStringOptions() {
        EncryptOptions options = new EncryptOptions("String").stringOptions(new StringOptions());
        assertTrue(options.toString().contains("stringOptions="));
    }
}
