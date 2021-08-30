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

package com.mongodb.client.model.search;

import com.mongodb.annotations.Immutable;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonValue;

@Immutable
public final class Fuzzy {
    @Nullable private final Integer maxEdits;
    @Nullable private final Integer prefixLength;
    @Nullable private final Integer maxExpansions;

    public static Fuzzy fuzzy() {
        return new Fuzzy(null, null, null);
    }

    Fuzzy(@Nullable final Integer maxEdits, @Nullable final Integer prefixLength, @Nullable final Integer maxExpansions) {
        this.maxEdits = maxEdits;
        this.prefixLength = prefixLength;
        this.maxExpansions = maxExpansions;
    }

    public Fuzzy maxEdits(int maxEdits) {
        return new Fuzzy(maxEdits, prefixLength, maxExpansions);
    }

    public Fuzzy prefixLength(int prefixLength) {
        return new Fuzzy(maxEdits, prefixLength, maxExpansions);
    }

    public Fuzzy maxExpansions(int maxExpansions) {
        return new Fuzzy(maxEdits, prefixLength, maxExpansions);
    }

    @Nullable
    public Integer getMaxEdits() {
        return maxEdits;
    }

    @Nullable
    public Integer getPrefixLength() {
        return prefixLength;
    }

    @Nullable
    public Integer getMaxExpansions() {
        return maxExpansions;
    }

    BsonValue toBsonValue() {
        BsonDocument document = new BsonDocument();

        if (maxEdits != null) {
             document.append("maxEdits", new BsonInt32(maxEdits));
        }

        if (prefixLength != null) {
            document.append("prefixLength", new BsonInt32(prefixLength));
        }

        if (maxExpansions != null) {
            document.append("maxExpansions", new BsonInt32(maxExpansions));
        }

        return document;
    }
}
