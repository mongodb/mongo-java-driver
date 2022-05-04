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
import org.bson.conversions.Bson;

import static com.mongodb.internal.client.model.Util.SEARCH_PATH_VALUE_KEY;
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
