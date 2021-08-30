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
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;

import static com.mongodb.assertions.Assertions.notNull;

@Immutable
public final class WildcardSearchPath extends SearchPath {
    private final String wildcard;

    WildcardSearchPath(final String wildcard) {
        this.wildcard = notNull("wildcard", wildcard);
    }

    public String getWildcard() {
        return wildcard;
    }

    @Override
    public BsonValue toBsonValue() {
        return new BsonDocument("wildcard", new BsonString(wildcard));
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WildcardSearchPath that = (WildcardSearchPath) o;

        return wildcard.equals(that.wildcard);
    }

    @Override
    public int hashCode() {
        return wildcard.hashCode();
    }
}
