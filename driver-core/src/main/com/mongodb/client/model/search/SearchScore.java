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
import com.mongodb.client.model.Projections;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.Document;
import org.bson.conversions.Bson;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static org.bson.assertions.Assertions.notNull;

/**
 * A modifier of the relevance score.
 * You may use the {@code $meta: "searchScore"} expression, e.g., via {@link Projections#metaSearchScore(String)},
 * to extract the relevance score assigned to each found document.
 *
 * @mongodb.atlas.manual atlas-search/scoring/ Scoring
 * @since 4.7
 */
@Evolving
public interface SearchScore extends Bson {
    /**
     * Returns a {@link SearchScore} that instructs to multiply the score by the specified {@code value}.
     *
     * @param value The positive value to multiply the score by.
     * @return The requested {@link SearchScore}.
     * @mongodb.atlas.manual atlas-search/scoring/#boost boost modifier
     */
    static ValueBoostSearchScore boost(final float value) {
        isTrueArgument("value must be positive", value > 0);
        return new SearchConstructibleBsonElement("boost", new BsonDocument("value", new BsonDouble(value)));
    }

    /**
     * Returns a {@link SearchScore} that instructs to multiply the score by the value of the specified field.
     *
     * @param path The numeric field whose value to multiply the score by.
     * @return The requested {@link SearchScore}.
     * @mongodb.atlas.manual atlas-search/scoring/#boost boost modifier
     */
    static PathBoostSearchScore boost(final FieldSearchPath path) {
        return new SearchConstructibleBsonElement("boost", new Document("path", notNull("value", (path)).toValue()));
    }

    /**
     * Returns a {@link SearchScore} that instructs to replace the score with the specified {@code value}.
     *
     * @param value The value to replace the score with.
     * @return The requested {@link SearchScore}.
     * @mongodb.atlas.manual atlas-search/scoring/#constant constant modifier
     */
    static ConstantSearchScore constant(final float value) {
        isTrueArgument("value must be positive", value > 0);
        return new SearchConstructibleBsonElement("constant", new BsonDocument("value", new BsonDouble(value)));
    }

    /**
     * Creates a {@link SearchScore} from a {@link Bson} in situations when there is no builder method that better satisfies your needs.
     * This method cannot be used to validate the syntax.
     * <p>
     * <i>Example</i><br>
     * The following code creates two functionally equivalent {@link SearchScore}s,
     * though they may not be {@linkplain Object#equals(Object) equal}.
     * <pre>{@code
     *  SearchScore score1 = SearchScore.boost(
     *      SearchPath.fieldPath("fieldName"));
     *  SearchScore score2 = SearchScore.of(new Document("boost",
     *      new Document("path", SearchPath.fieldPath("fieldName").toValue())));
     * }</pre>
     *
     * @param score A {@link Bson} representing the required {@link SearchScore}.
     * @return The requested {@link SearchScore}.
     */
    static SearchScore of(final Bson score) {
        return new SearchConstructibleBson(notNull("score", score));
    }
}
