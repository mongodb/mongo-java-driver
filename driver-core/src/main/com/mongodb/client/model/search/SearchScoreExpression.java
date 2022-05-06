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

import com.mongodb.annotations.Evolving;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.internal.client.model.Util.SEARCH_PATH_VALUE_KEY;
import static com.mongodb.internal.client.model.Util.sizeAtLeast;
import static org.bson.assertions.Assertions.notNull;

/**
 * @see SearchScore#function(SearchScoreExpression)
 * @mongodb.atlas.manual atlas-search/scoring/#expressions Expressions for the function score modifier
 * @since 4.7
 */
@Evolving
public interface SearchScoreExpression extends Bson {
    /**
     * Returns a {@link SearchScoreExpression} that evaluates into the relevance score of a document.
     *
     * @return The requested {@link SearchScoreExpression}.
     */
    static RelevanceSearchScoreExpression relevanceExpression() {
        return new SearchConstructibleBson(new BsonDocument("score", new BsonString("relevance")));
    }

    /**
     * Returns a {@link SearchScoreExpression} that evaluates into the value of the specified field.
     *
     * @param path The numeric field whose value to use as the result of the expression.
     * @return The requested {@link SearchScoreExpression}.
     * @see SearchScore#boost(FieldSearchPath)
     */
    static PathSearchScoreExpression pathExpression(final FieldSearchPath path) {
        return new SearchConstructibleBsonElement("path", new Document(SEARCH_PATH_VALUE_KEY, notNull("path", path).toValue()));
    }

    /**
     * Returns a {@link SearchScoreExpression} that evaluates into the specified {@code value}.
     *
     * @param value The value to use as the result of the expression. Unlike {@link SearchScore#constant(float)}, does not have constraints.
     * @return The requested {@link SearchScoreExpression}.
     * @see SearchScore#constant(float)
     */
    static ConstantSearchScoreExpression constantExpression(final float value) {
        return new SearchConstructibleBson(new BsonDocument("constant", new BsonDouble(value)));
    }

    /**
     * Returns a {@link SearchScoreExpression} that represents a Gaussian function whose output is within the interval [0, 1].
     * Roughly speaking, the further the value of the {@code path} expression is from the {@code origin},
     * the smaller the output of the function.
     * <p>
     * The {@code scale} and {@link GaussSearchScoreExpression#decay(double) decay} are parameters of the Gaussian function,
     * they define the rate at which the function decays.
     * The input of the Gaussian function is the output of another function:
     * max(0, abs({@code pathValue} - {@code origin}) - {@link GaussSearchScoreExpression#offset(double) offset}),
     * where {@code pathValue} is the value of the {@code path} expression.</p>
     *
     * @param origin The point of origin, see {@link GaussSearchScoreExpression#offset(double)}.
     * The value of the Gaussian function is 1 if the value of the {@code path} expression is {@code origin}.
     * @param path The expression whose value is used to calculate the input of the Gaussian function.
     * @param scale The non-zero distance from the points {@code origin} ± {@link GaussSearchScoreExpression#offset(double) offset}
     * at which the output of the Gaussian function must decay by the factor of {@link GaussSearchScoreExpression#decay(double) decay}.
     * @return The requested {@link SearchScoreExpression}.
     */
    static GaussSearchScoreExpression gaussExpression(final double origin, final PathSearchScoreExpression path, final double scale) {
        notNull("path", path);
        isTrueArgument("scale must not be 0", scale != 0);
        Bson value = new Bson() {
            @Override
            public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
                BsonDocument pathDoc = path.toBsonDocument(documentClass, codecRegistry);
                assertTrue(pathDoc.size() == 1);
                return new BsonDocument("origin", new BsonDouble(origin))
                        .append("path", pathDoc.values().iterator().next())
                        .append("scale", new BsonDouble(scale));
            }

            @Override
            public String toString() {
                return "{\"origin\": " + origin
                        + ", \"path\": " + path
                        + ", \"scale\": " + scale
                        + '}';
            }
        };
        return new SearchConstructibleBsonElement("gauss", value);
    }

    /**
     * Returns a {@link SearchScoreExpression} that evaluates into log10({@code expressionValue}),
     * where {@code expressionValue} is the value of the {@code expression}.
     *
     * @param expression The expression whose value is the input of the log10 function.
     * @return The requested {@link SearchScoreExpression}.
     */
    static LogSearchScoreExpression logExpression(final SearchScoreExpression expression) {
        return new SearchConstructibleBson(new Document("log", notNull("expression", expression)));
    }

    /**
     * Returns a {@link SearchScoreExpression} that evaluates into log10({@code expressionValue} + 1),
     * where {@code expressionValue} is the value of the {@code expression}.
     *
     * @param expression The expression whose value is used to calculate the input of the log10 function.
     * @return The requested {@link SearchScoreExpression}.
     */
    static Log1pSearchScoreExpression log1pExpression(final SearchScoreExpression expression) {
        return new SearchConstructibleBson(new Document("log1p", notNull("expression", expression)));
    }

    /**
     * Returns a {@link SearchScoreExpression} that evaluates into the sum of the values of the specified {@code expressions}.
     *
     * @param expressions The expressions whose values to add. Must contain at least two expressions.
     * @return The requested {@link SearchScoreExpression}.
     */
    static AddSearchScoreExpression addExpression(final Iterable<? extends SearchScoreExpression> expressions) {
        notNull("expressions", expressions);
        isTrueArgument("expressions must contain at least 2 elements", sizeAtLeast(expressions, 2));
        return new SearchConstructibleBson(new Document("add", expressions));
    }

    /**
     * Returns a {@link SearchScoreExpression} that evaluates into the product of the values of the specified {@code expressions}.
     *
     * @param expressions The expressions whose values to multiply. Must contain at least two expressions.
     * @return The requested {@link SearchScoreExpression}.
     */
    static MultiplySearchScoreExpression multiplyExpression(final Iterable<? extends SearchScoreExpression> expressions) {
        notNull("expressions", expressions);
        isTrueArgument("expressions must contain at least 2 elements", sizeAtLeast(expressions, 2));
        return new SearchConstructibleBson(new Document("multiply", expressions));
    }

    /**
     * Creates a {@link SearchScoreExpression} from a {@link Bson} in situations when there is no builder method
     * that better satisfies your needs.
     * This method cannot be used to validate the syntax.
     * <p>
     * <i>Example</i><br>
     * The following code creates two functionally equivalent {@link SearchScoreExpression}s,
     * though they may not be {@linkplain Object#equals(Object) equal}.
     * <pre>{@code
     *  SearchScoreExpression expression1 = SearchScoreExpression.pathExpression(
     *          SearchPath.fieldPath("fieldName"))
     *          .undefined(-1.5f);
     *  SearchScoreExpression expression2 = new Document("path",
     *          new Document("value", SearchPath.fieldPath("fieldName").toValue())
     *                  .append("undefined", -1.5));
     * }</pre>
     *
     * @param expression A {@link Bson} representing the required {@link SearchScoreExpression}.
     * @return The requested {@link SearchScoreExpression}.
     */
    static SearchScoreExpression of(final Bson expression) {
        return new SearchConstructibleBson(notNull("expression", expression));
    }
}
