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
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.Map;
import java.util.Objects;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.Collections.unmodifiableMap;

@Immutable
public final class DocumentExpression implements Expression {
    private final Map<String, Expression> elements;

    DocumentExpression(final Map<String, Expression> elements) {
        this.elements = notNull("elements", elements);
    }

    public Map<String, Expression> getElements() {
        return unmodifiableMap(elements);
    }


    @Override
    public BsonValue toBsonValue(final CodecRegistry codecRegistry) {
        BsonDocument value = new BsonDocument();
        elements.forEach((key, value1) -> value.append(key, value1.toBsonValue(codecRegistry)));
        return value;
    }

    @Override
    public String toString() {
        return "DocumentExpression{" +
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
        DocumentExpression that = (DocumentExpression) o;
        return Objects.equals(elements, that.elements);
    }

    @Override
    public int hashCode() {
        return Objects.hash(elements);
    }
}
