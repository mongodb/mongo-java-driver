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

import com.mongodb.annotations.Immutable;
import org.bson.BsonArray;
import org.bson.BsonValue;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.List;
import java.util.Objects;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.Collections.unmodifiableList;

@Immutable
public final class ArrayExpression implements Expression {
    private final List<Expression> elements;

    ArrayExpression(final List<Expression> elements) {
        this.elements = notNull("elements", elements);
    }

    public List<Expression> getElements() {
        return unmodifiableList(elements);
    }

    @Override
    public BsonValue toBsonValue(final CodecRegistry codecRegistry) {
        BsonArray value = new BsonArray();
        elements.forEach(expression -> value.add(expression.toBsonValue(codecRegistry)));
        return value;
    }

    @Override
    public String toString() {
        return "ArrayExpression{" +
                "elements=" + elements +
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
        ArrayExpression that = (ArrayExpression) o;
        return Objects.equals(elements, that.elements);
    }

    @Override
    public int hashCode() {
        return Objects.hash(elements);
    }
}
