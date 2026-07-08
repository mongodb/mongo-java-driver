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

package com.mongodb.internal.client.vault;

import com.mongodb.client.model.vault.EncryptOptions;
import com.mongodb.client.model.vault.StringOptions;
import com.mongodb.client.model.vault.TextOptions;
import com.mongodb.internal.crypt.capi.MongoExplicitEncryptOptions;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class EncryptOptionsHelperTest {

    @Test
    void shouldMapStringOptionsToTextOptionsDocument() {
        EncryptOptions options = new EncryptOptions("String").stringOptions(new StringOptions()
                .caseSensitive(true)
                .diacriticSensitive(false)
                .prefixOptions(BsonDocument.parse("{strMaxQueryLength: 10, strMinQueryLength: 2}")));

        MongoExplicitEncryptOptions result = EncryptOptionsHelper.asMongoExplicitEncryptOptions(options);

        assertEquals(BsonDocument.parse("{caseSensitive: true, diacriticSensitive: false, "
                        + "prefix: {strMaxQueryLength: 10, strMinQueryLength: 2}}"),
                result.getTextOptions());
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldFallBackToDeprecatedTextOptions() {
        EncryptOptions options = new EncryptOptions("String").textOptions(new TextOptions()
                .caseSensitive(true)
                .diacriticSensitive(true)
                .suffixOptions(BsonDocument.parse("{strMaxQueryLength: 10, strMinQueryLength: 2}")));

        MongoExplicitEncryptOptions result = EncryptOptionsHelper.asMongoExplicitEncryptOptions(options);

        assertEquals(BsonDocument.parse("{caseSensitive: true, diacriticSensitive: true, "
                        + "suffix: {strMaxQueryLength: 10, strMinQueryLength: 2}}"),
                result.getTextOptions());
    }

    @Test
    void shouldLeaveTextOptionsNullWhenNeitherSet() {
        MongoExplicitEncryptOptions result = EncryptOptionsHelper.asMongoExplicitEncryptOptions(
                new EncryptOptions("Indexed"));
        assertNull(result.getTextOptions());
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldPreferStringOptionsOverDeprecatedTextOptionsWhenBothSet() {
        EncryptOptions options = new EncryptOptions("String")
                .stringOptions(new StringOptions()
                        .caseSensitive(true)
                        .diacriticSensitive(true)
                        .prefixOptions(BsonDocument.parse("{strMaxQueryLength: 10, strMinQueryLength: 2}")))
                .textOptions(new TextOptions()
                        .caseSensitive(false)
                        .diacriticSensitive(false)
                        .suffixOptions(BsonDocument.parse("{strMaxQueryLength: 5, strMinQueryLength: 1}")));

        MongoExplicitEncryptOptions result = EncryptOptionsHelper.asMongoExplicitEncryptOptions(options);

        assertEquals(BsonDocument.parse("{caseSensitive: true, diacriticSensitive: true, "
                        + "prefix: {strMaxQueryLength: 10, strMinQueryLength: 2}}"),
                result.getTextOptions());
    }
}
