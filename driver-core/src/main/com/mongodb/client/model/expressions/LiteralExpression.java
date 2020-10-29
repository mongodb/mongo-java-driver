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
import org.bson.BsonDocumentWriter;
import org.bson.BsonValue;
import org.bson.codecs.Codec;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.Objects;

@Immutable
public class LiteralExpression implements Expression {

    private final Object value;
    private final boolean parsed;

    LiteralExpression(final Object value, final boolean parsed) {
        this.value = value;
        this.parsed = parsed;
    }

    public Object getValue() {
        return value;
    }

    public boolean isParsed() {
        return parsed;
    }

    @Override
    public BsonValue toBsonValue(final CodecRegistry codecRegistry) {
        BsonDocument unparsedLiteral = convertToUnparsedLiteral(codecRegistry);
        if (isParsed()) {
            return unparsedLiteral.get("$literal");
        } else {
            return unparsedLiteral;
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private BsonDocument convertToUnparsedLiteral(final CodecRegistry codecRegistry) {
        if (value instanceof BsonValue) {
            return new BsonDocument("$literal", (BsonValue) value);
        } else {
            BsonDocument wrapper = new BsonDocument();
            BsonDocumentWriter writer = new BsonDocumentWriter(wrapper);
            writer.writeStartDocument();
            writer.writeName("$literal");
            if (value == null) {
                writer.writeNull();
            } else {
                Codec codec = codecRegistry.get(value.getClass());
                codec.encode(writer, value, EncoderContext.builder().build());
            }
            return wrapper;
        }
    }

    @Override
    public String toString() {
        return "LiteralExpression{" +
                "value=" + value +
                ", parsed=" + parsed +
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
        LiteralExpression that = (LiteralExpression) o;
        return parsed == that.parsed &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, parsed);
    }
}
