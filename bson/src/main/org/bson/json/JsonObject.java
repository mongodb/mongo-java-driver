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

import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import static org.bson.assertions.Assertions.isTrueArgument;
import static org.bson.assertions.Assertions.notNull;

/**
 * A wrapper class that holds a JSON object string. This class makes decoding JSON efficient.
 * Note that this class only holds valid JSON objects, not arrays or other values.
 *
 * @since 4.2
 */
public class JsonObject implements Bson {
    private final String json;

    /**
     * Constructs a new instance with the given JSON object string. Clients must ensure
     * they only pass in valid JSON objects to this constructor. The constructor does not
     * perform full validation on construction, but an invalid JsonObject can cause errors
     * when it is used later on.
     *
     * @param json the JSON object string
     */
    public JsonObject(final String json) {
        notNull("Json", json);

        boolean foundBrace = false;
        for (int i = 0; i < json.length(); i++) {
            char c = json.charAt(i);
            if (c == '{') {
                foundBrace = true;
                break;
            }
            isTrueArgument("json is a valid JSON object", Character.isWhitespace(c));
        }
        isTrueArgument("json is a valid JSON object", foundBrace);

        this.json = json;
    }

    /**
     * Gets the JSON object string
     *
     * @return the JSON object string
     */
    public String getJson() {
        return json;
    }

    @Override
    public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry registry) {
        return new BsonDocumentWrapper<>(this, registry.get(JsonObject.class));
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        JsonObject that = (JsonObject) o;

        if (!json.equals(that.getJson())) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return json.hashCode();
    }

    @Override
    public String toString() {
        return json;
    }
}
