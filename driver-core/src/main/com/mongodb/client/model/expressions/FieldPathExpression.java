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

package com.mongodb.client.model.expressions;

import jdk.nashorn.internal.ir.annotations.Immutable;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.Objects;

import static com.mongodb.assertions.Assertions.notNull;

@Immutable
public class FieldPathExpression implements Expression {
    private final String fieldPath;

    FieldPathExpression(final String fieldPath) {
        this.fieldPath = notNull("fieldPath", fieldPath);
    }

    public String getFieldPath() {
        return fieldPath;
    }

    @Override
    public BsonValue toBsonValue(final CodecRegistry codecRegistry) {
        return new BsonString("$" + fieldPath);
    }

    @Override
    public String toString() {
        return "PathExpression{" +
                "path='" + fieldPath + '\'' +
                '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FieldPathExpression that = (FieldPathExpression) o;
        return Objects.equals(fieldPath, that.fieldPath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldPath);
    }
}
