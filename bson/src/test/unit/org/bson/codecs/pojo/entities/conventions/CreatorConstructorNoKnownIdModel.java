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

package org.bson.codecs.pojo.entities.conventions;

import org.bson.codecs.pojo.annotations.BsonCreator;
import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.codecs.pojo.annotations.BsonProperty;

import java.util.Objects;

public class CreatorConstructorNoKnownIdModel {
    private final String stringField;
    private final long longField;

    @BsonCreator
    public CreatorConstructorNoKnownIdModel(
            @BsonId final String stringField,
            @BsonProperty("longField") final long longField) {
        this.stringField = stringField;
        this.longField = longField;
    }

    public String getStringField() {
        return stringField;
    }

    public long getLongField() {
        return longField;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final CreatorConstructorNoKnownIdModel that = (CreatorConstructorNoKnownIdModel) o;
        return longField == that.longField && Objects.equals(stringField, that.stringField);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stringField, longField);
    }

    @Override
    public String toString() {
        return "CreatorConstructorNoKnownIdModel{"
                + ", stringField='" + stringField + '\''
                + ", longField=" + longField
                + '}';
    }
}
