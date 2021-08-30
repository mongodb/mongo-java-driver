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
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.conversions.Bson;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * @since 4.4
 */
@Immutable
public final class FieldSearchPath extends SearchPath {
    private final String path;
    @Nullable
    private final String multi;

    FieldSearchPath(final String path, @Nullable final String multi) {
        this.path = notNull("path", path);
        this.multi = multi;
    }

    FieldSearchPath multi(String multi) {
        return new FieldSearchPath(path, multi);
    }

    public String getPath() {
        return path;
    }

    @Nullable
    public String getMulti() {
        return multi;
    }

    @Override
    public BsonValue toBsonValue() {
        if (multi == null) {
            return new BsonString(path);
        } else {
            return new BsonDocument("value", new BsonString(path)).append("multi", new BsonString(multi));
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FieldSearchPath that = (FieldSearchPath) o;

        return path.equals(that.path);
    }

    @Override
    public int hashCode() {
        return path.hashCode();
    }
}
