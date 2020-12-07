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
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.client.model.expressions.ExpressionHelper.toBsonArray;

@Immutable
public final class AddExpression implements Expression {
    private final List<Expression> numbers;

    AddExpression(final List<Expression> numbers) {
        this.numbers = notNull("numbers", numbers);
    }

    public List<Expression> getNumbers() {
        return Collections.unmodifiableList(numbers);
    }

    @Override
    public BsonValue toBsonValue(final CodecRegistry codecRegistry) {
        return new BsonDocument("$add", toBsonArray(numbers, codecRegistry));
    }

    @Override
    public String toString() {
        return "AddExpression{" +
                "numbers=" + numbers +
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
        AddExpression that = (AddExpression) o;
        return numbers.equals(that.numbers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(numbers);
    }
}
