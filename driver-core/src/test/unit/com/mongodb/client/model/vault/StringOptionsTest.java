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

import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StringOptionsTest {

    @Test
    void shouldRoundTripAllProperties() {
        BsonDocument prefix = BsonDocument.parse("{strMaxQueryLength: 10, strMinQueryLength: 2}");
        BsonDocument suffix = BsonDocument.parse("{strMaxQueryLength: 10, strMinQueryLength: 2}");
        BsonDocument substring = BsonDocument.parse("{strMaxLength: 10, strMaxQueryLength: 10, strMinQueryLength: 2}");

        StringOptions options = new StringOptions()
                .caseSensitive(true)
                .diacriticSensitive(true)
                .prefixOptions(prefix)
                .suffixOptions(suffix)
                .substringOptions(substring);

        assertTrue(options.getCaseSensitive());
        assertTrue(options.getDiacriticSensitive());
        assertEquals(prefix, options.getPrefixOptions());
        assertEquals(suffix, options.getSuffixOptions());
        assertEquals(substring, options.getSubstringOptions());
    }

    @Test
    void shouldDefaultOptionDocumentsToNull() {
        StringOptions options = new StringOptions();
        assertNull(options.getPrefixOptions());
        assertNull(options.getSuffixOptions());
        assertNull(options.getSubstringOptions());
    }

    @Test
    void shouldDefaultBooleanFlagsToFalse() {
        StringOptions options = new StringOptions();
        assertFalse(options.getCaseSensitive());
        assertFalse(options.getDiacriticSensitive());
    }

    @Test
    void shouldIncludeAllFieldsInToString() {
        String result = new StringOptions()
                .caseSensitive(true)
                .diacriticSensitive(false)
                .prefixOptions(BsonDocument.parse("{strMaxQueryLength: 10, strMinQueryLength: 2}"))
                .toString();

        assertEquals("StringOptions{caseSensitive=true, diacriticSensitive=false, "
                + "prefixOptions={\"strMaxQueryLength\": 10, \"strMinQueryLength\": 2}, "
                + "suffixOptions=null, substringOptions=null}", result);
    }
}
