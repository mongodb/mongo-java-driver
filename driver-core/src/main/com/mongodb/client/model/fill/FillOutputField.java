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
package com.mongodb.client.model.fill;

import com.mongodb.annotations.Evolving;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.WindowOutputFields;
import org.bson.Document;
import org.bson.conversions.Bson;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * The core part of the {@code $fill} pipeline stage of an aggregation pipeline.
 * A pair of an expression/method and a path to a field to be filled with evaluation results of the expression/method.
 *
 * @see Aggregates#fill(FillOptions, Iterable)
 * @see Aggregates#fill(FillOptions, FillOutputField, FillOutputField...)
 * @mongodb.server.release 5.3
 * @since 4.7
 */
@Evolving
public interface FillOutputField extends Bson {
    /**
     * Returns a {@link FillOutputField} that uses the specified {@code expression}.
     *
     * @param field The field to fill.
     * @param expression The expression.
     * @param <TExpression> The {@code expression} type.
     * @return The requested {@link FillOutputField}.
     * @mongodb.driver.manual core/document/#dot-notation Dot notation
     */
    static <TExpression> ValueFillOutputField value(final String field, TExpression expression) {
        return new FillConstructibleBsonElement(notNull("field", field),
                new Document("value", (notNull("expression", expression))));
    }

    /**
     * Returns a {@link FillOutputField} that uses the {@link WindowOutputFields#locf(String, Object) locf} method.
     *
     * @param field The field to fill.
     * @return The requested {@link FillOutputField}.
     * @mongodb.driver.manual core/document/#dot-notation Dot notation
     */
    static LocfFillOutputField locf(final String field) {
        return new FillConstructibleBsonElement(notNull("field", field),
                new Document("method", "locf"));
    }

    /**
     * Returns a {@link FillOutputField} that uses the {@link WindowOutputFields#linearFill(String, Object) linear} method.
     * <p>
     * {@linkplain FillOptions#sortBy(Bson) Sorting} is required.</p>
     *
     * @param field The field to fill.
     * @return The requested {@link FillOutputField}.
     * @mongodb.driver.manual core/document/#dot-notation Dot notation
     */
    static LinearFillOutputField linear(final String field) {
        return new FillConstructibleBsonElement(notNull("field", field),
                new Document("method", "linear"));
    }

    /**
     * Creates a {@link FillOutputField} from a {@link Bson} in situations when there is no builder method
     * that better satisfies your needs.
     * This method cannot be used to validate the syntax.
     * <p>
     * <i>Example</i><br>
     * The following code creates two functionally equivalent {@link FillOutputField}s,
     * though they may not be {@linkplain Object#equals(Object) equal}.
     * <pre>{@code
     *  FillOutputField field1 = FillOutputField.locf("fieldName");
     *  FillOutputField field2 = FillOutputField.of(new Document("fieldName", new Document("method", "locf")));
     * }</pre>
     *
     * @param fill A {@link Bson} representing the required {@link FillOutputField}.
     * @return The requested {@link FillOutputField}.
     */
    static FillOutputField of(final Bson fill) {
        return new FillConstructibleBsonElement(notNull("fill", fill));
    }
}
