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

package org.bson.json;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.bson.json.UuidStringValidator.validate;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class UuidStringValidatorTest {

    @ParameterizedTest
    @ValueSource(strings = {
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "cccccccc-cccc-cccc-cccc-cccccccccccc",
            "dddddddd-dddd-dddd-dddd-dddddddddddd",
            "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee",
            "ffffffff-ffff-ffff-ffff-ffffffffffff",
            "AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA",
            "BBBBBBBB-BBBB-BBBB-BBBB-BBBBBBBBBBBB",
            "CCCCCCCC-CCCC-CCCC-CCCC-CCCCCCCCCCCC",
            "DDDDDDDD-DDDD-DDDD-DDDD-DDDDDDDDDDDD",
            "EEEEEEEE-EEEE-EEEE-EEEE-EEEEEEEEEEEE",
            "FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF",
            "00000000-0000-0000-0000-000000000000",
            "11111111-1111-1111-1111-111111111111",
            "22222222-2222-2222-2222-222222222222",
            "33333333-3333-3333-3333-333333333333",
            "44444444-4444-4444-4444-444444444444",
            "55555555-5555-5555-5555-555555555555",
            "66666666-6666-6666-6666-666666666666",
            "77777777-7777-7777-7777-777777777777",
            "88888888-8888-8888-8888-888888888888",
            "99999999-9999-9999-9999-999999999999"})
    public void testValidUuidStrings(final String uuidString) {
        assertDoesNotThrow(() -> validate(uuidString));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa",
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaaa",
            "aaaaaaaa+aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "aaaaaaaa-aaaa+aaaa-aaaa-aaaaaaaaaaaa",
            "aaaaaaaa-aaaa-aaaa+aaaa-aaaaaaaaaaaa",
            "aaaaaaaa-aaaa-aaaa-aaaa+aaaaaaaaaaaa",
            "`aaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "{aaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "@aaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "[aaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "/aaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            ":aaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "a:aaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "aa:aaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "aaa:aaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "aaaa:aaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "aaaaa:aa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "aaaaaa:a-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "aaaaaaaa-:aaa-aaaa-aaaa-aaaaaaaaaaaa",
            "aaaaaaaa-a:aa-aaaa-aaaa-aaaaaaaaaaaa",
            "aaaaaaaa-aa:a-aaaa-aaaa-aaaaaaaaaaaa",
            "aaaaaaaa-aaa:-aaaa-aaaa-aaaaaaaaaaaa",
            "aaaaaaaa-aaaa-:aaa-aaaa-aaaaaaaaaaaa",
            "aaaaaaaa-aaaa-a:aa-aaaa-aaaaaaaaaaaa",
            "aaaaaaaa-aaaa-aa:a-aaaa-aaaaaaaaaaaa",
            "aaaaaaaa-aaaa-aaa:-aaaa-aaaaaaaaaaaa",
            "aaaaaaaa-aaaa-aaaa-:aaa-aaaaaaaaaaaa",
            "aaaaaaaa-aaaa-aaaa-a:aa-aaaaaaaaaaaa",
            "aaaaaaaa-aaaa-aaaa-aa:a-aaaaaaaaaaaa",
            "aaaaaaaa-aaaa-aaaa-aaa:-aaaaaaaaaaaa",
            "aaaaaaaa-aaaa-aaaa-aaaa-:aaaaaaaaaaa",
            "aaaaaaaa-aaaa-aaaa-aaaa-a:aaaaaaaaaa",
            "aaaaaaaa-aaaa-aaaa-aaaa-aa:aaaaaaaaa",
            "aaaaaaaa-aaaa-aaaa-aaaa-aaa:aaaaaaaa",
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaa:aaaaaaa",
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaa:aaaaaa",
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaa:aaaaa",
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaa:aaaa",
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaa:aaa",
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaa:aa",
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaa:a",
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa:"})
    public void testInvalidUuidStrings(final String uuidString) {
        assertThrows(IllegalArgumentException.class, () -> validate(uuidString));
    }
}
