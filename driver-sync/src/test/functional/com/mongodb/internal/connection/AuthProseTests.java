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

package com.mongodb.internal.connection;

import com.mongodb.MongoCredential;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * See
 * <a href="https://github.com/mongodb/specifications/blob/master/source/auth/tests/README.md#prose-tests">Prose Tests</a>.
 */
class AuthProseTests {
    @DisplayName("1. Driver rejects a PLAIN authentication credential with NUL character")
    @ParameterizedTest
    @CsvSource({
            "user\0name,password",
            "username,pass\0word"
    })
    void testNoNulInPlainCredentials(final String username, final String password) {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> MongoCredential.createPlainCredential(username, "source", password.toCharArray()));
        assertTrue(ex.getMessage().contains("cannot contain NUL characters"));
    }
}
