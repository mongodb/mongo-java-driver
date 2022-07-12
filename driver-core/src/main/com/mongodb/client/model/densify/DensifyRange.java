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
package com.mongodb.client.model.densify;

import com.mongodb.annotations.Evolving;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.MongoTimeUnit;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.time.Instant;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.Arrays.asList;

/**
 * A specification of how to compute the missing {@linkplain Aggregates#densify(String, DensifyRange, DensifyOptions) field} values
 * for which new documents must be added. It specifies a half-closed interval of values with the lower bound being inclusive, and a step.
 * The first potentially missing value within each interval is its lower bound, other values are computed by adding the step
 * multiple times, until the result is out of the interval. Each time the step is added, the result is a potentially missing value for
 * which a new document must be added if the sequence of documents that is being densified does not have a document
 * with equal value of the field.
 *
 * @see Aggregates#densify(String, DensifyRange, DensifyOptions)
 * @see Aggregates#densify(String, DensifyRange)
 * @mongodb.server.release 5.1
 * @since 4.7
 */
@Evolving
public interface DensifyRange extends Bson {
    /**
     * Returns a {@link DensifyRange} that represents an interval with the smallest
     * BSON {@link BsonType#INT32 32-bit integer} / {@link BsonType#INT64 64-bit integer} / {@link BsonType#DOUBLE Double} /
     * {@link BsonType#DECIMAL128 Decimal128} value of the {@linkplain Aggregates#densify(String, DensifyRange, DensifyOptions) field}
     * in the sequence of documents being its lower bound, and the largest value being the upper bound.
     *
     * @param step The step.
     * @return The requested {@link DensifyRange}.
     */
    static NumberDensifyRange fullRangeWithStep(final Number step) {
        return new DensifyConstructibleBson(new Document("bounds", "full")
                .append("step", notNull("step", step)));
    }

    /**
     * Returns a {@link DensifyRange} that represents an interval with the smallest
     * BSON {@link BsonType#INT32 32-bit integer} / {@link BsonType#INT64 64-bit integer} / {@link BsonType#DOUBLE Double} /
     * {@link BsonType#DECIMAL128 Decimal128} value of the {@linkplain Aggregates#densify(String, DensifyRange, DensifyOptions) field}
     * in the {@linkplain DensifyOptions#partitionByFields(Iterable) partition} of documents being its lower bound,
     * and the largest value being the upper bound.
     *
     * @param step The step.
     * @return The requested {@link DensifyRange}.
     */
    static NumberDensifyRange partitionRangeWithStep(final Number step) {
        return new DensifyConstructibleBson(new Document("bounds", "partition")
                .append("step", notNull("step", step)));
    }

    /**
     * Returns a {@link DensifyRange} that represents a single interval [l, u).
     *
     * @param l The lower bound.
     * @param u The upper bound.
     * @param step The step.
     * @return The requested {@link DensifyRange}.
     */
    static NumberDensifyRange rangeWithStep(final Number l, final Number u, final Number step) {
        notNull("l", l);
        notNull("u", u);
        notNull("step", step);
        return new DensifyConstructibleBson(new Document("bounds", asList(l, u))
                .append("step", notNull("step", step)));
    }

    /**
     * Returns a {@link DensifyRange} that represents an interval with the smallest BSON {@link BsonType#DATE_TIME Date} value
     * of the {@linkplain Aggregates#densify(String, DensifyRange, DensifyOptions) field}
     * in the sequence of documents being its lower bound, and the largest value being the upper bound.
     *
     * @param step The step.
     * @param unit The unit in which the {@code step} is specified.
     * @return The requested {@link DensifyRange}.
     */
    static DateDensifyRange fullRangeWithStep(final long step, final MongoTimeUnit unit) {
        notNull("unit", unit);
        return new DensifyConstructibleBson(new BsonDocument("bounds", new BsonString("full"))
                .append("step", new BsonInt64(step))
                .append("unit", new BsonString(unit.value())));
    }

    /**
     * Returns a {@link DensifyRange} that represents an interval with the smallest BSON {@link BsonType#DATE_TIME Date} value
     * of the {@linkplain Aggregates#densify(String, DensifyRange, DensifyOptions) field}
     * in the {@linkplain DensifyOptions#partitionByFields(Iterable) partition} of documents being its lower bound,
     * and the largest value being the upper bound.
     *
     * @param step The step.
     * @param unit The unit in which the {@code step} is specified.
     * @return The requested {@link DensifyRange}.
     */
    static DateDensifyRange partitionRangeWithStep(final long step, final MongoTimeUnit unit) {
        notNull("unit", unit);
        return new DensifyConstructibleBson(new BsonDocument("bounds", new BsonString("partition"))
                .append("step", new BsonInt64(step))
                .append("unit", new BsonString(unit.value())));
    }

    /**
     * Returns a {@link DensifyRange} that represents a single interval [l, u).
     *
     * @param l The lower bound.
     * @param u The upper bound.
     * @param step The step.
     * @param unit The unit in which the {@code step} is specified.
     * @return The requested {@link DensifyRange}.
     */
    static DateDensifyRange rangeWithStep(final Instant l, final Instant u, final long step, final MongoTimeUnit unit) {
        notNull("l", l);
        notNull("u", u);
        notNull("unit", unit);
        return new DensifyConstructibleBson(new Document("bounds", asList(l, u))
                .append("step", step)
                .append("unit", unit.value()));
    }

    /**
     * Creates a {@link DensifyRange} from a {@link Bson} in situations when there is no builder method that better satisfies your needs.
     * This method cannot be used to validate the syntax.
     * <p>
     * <i>Example</i><br>
     * The following code creates two functionally equivalent {@link DensifyRange}s,
     * though they may not be {@linkplain Object#equals(Object) equal}.
     * <pre>{@code
     *  DensifyRange range1 = DensifyRange.partitionRangeWithStep(
     *          1, MongoTimeUnit.MINUTE);
     *  DensifyRange range2 = DensifyRange.of(new Document("bounds", "partition")
     *          .append("step", 1).append("unit", MongoTimeUnit.MINUTE.value()));
     * }</pre>
     *
     * @param range A {@link Bson} representing the required {@link DensifyRange}.
     * @return The requested {@link DensifyRange}.
     */
    static DensifyRange of(final Bson range) {
        return new DensifyConstructibleBson(notNull("range", range));
    }
}
