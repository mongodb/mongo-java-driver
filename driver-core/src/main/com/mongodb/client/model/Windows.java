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
package com.mongodb.client.model;

import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWriter;
import org.bson.BsonType;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.bson.types.Decimal128;

import java.util.Objects;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.isTrueArgument;
import static org.bson.assertions.Assertions.notNull;

/**
 * Builders for {@linkplain Window windows} used when expressing {@linkplain WindowedComputation windowed computations}.
 * There are two types of windows: {@linkplain #documents(int, int) documents} and {@linkplain #range(long, long) range}.
 * <p>
 * Bounded and half-bounded windows require sorting.
 * Window bounds are inclusive and the lower bound must always be less than or equal to the upper bound.
 * The following type-specific rules are applied to windows:
 * <ul>
 *     <li>documents
 *         <ul>
 *             <li>bounds
 *                 <ul>
 *                     <li>0 refers to the current document and is functionally equivalent to {@link Bound#CURRENT};</li>
 *                     <li>a negative value refers to documents preceding the current one;</li>
 *                     <li>a positive value refers to documents following the current one;</li>
 *                 </ul>
 *             </li>
 *         </ul>
 *     </li>
 *     <li>range
 *         <ul>
 *             <li>{@link Aggregates#setWindowFields(Object, Bson, Iterable) sortBy}
 *                 <ul>
 *                     <li>must contain exactly one field;</li>
 *                     <li>must specify the ascending sort order;</li>
 *                     <li>the {@code sortBy} field must be of either a numeric BSON type
 *                     (see the {@code $isNumber} aggregation pipeline stage)
 *                     or the BSON {@link BsonType#DATE_TIME Date} type if {@linkplain #timeRange(long, long, MongoTimeUnit) time}
 *                     bounds are used;</li>
 *                 </ul>
 *             </li>
 *             <li>bounds
 *                 <ul>
 *                     <li>if numeric, i.e., not {@link Bound}, then the bound is calculated by adding
 *                     the value to the value of the {@code sortBy} field in the current document;</li>
 *                     <li>if {@link Bound#CURRENT}, then the bound is determined by the current document
 *                     and not the current value of the {@code sortBy} field;</li>
 *                     <li>time bounds require specifying a {@linkplain MongoTimeUnit time unit} and are added as per the
 *                     {@code $dateAdd}/{@code $dateSubtract} aggregation pipeline stage specification.</li>
 *                 </ul>
 *             </li>
 *         </ul>
 *     </li>
 * </ul>
 *
 * @see WindowedComputation
 * @mongodb.driver.manual reference/operator/aggregation/isNumber/ $isNumber aggregation pipeline stage
 * @mongodb.driver.manual reference/bson-types/#date BSON Date type
 * @mongodb.driver.manual reference/operator/aggregation/dateAdd/ $dateAdd aggregation pipeline stage
 * @mongodb.driver.manual reference/operator/aggregation/dateSubtract/ $dateSubtract aggregation pipeline stage
 * @mongodb.server.release 5.0
 * @since 4.3
 */
public final class Windows {
    /**
     * Creates a window from {@link Bson} in situations when there is no builder method that better satisfies your needs.
     * This method cannot be used to validate the syntax.
     * <p>
     * <i>Example</i><br>
     * The following code creates two functionally equivalent windows, though they may not be {@linkplain #equals(Object) equal}.
     * <pre>{@code
     *  Window pastWeek1 = Windows.timeRange(-1, MongoTimeUnit.WEEK, Windows.Bound.CURRENT);
     *  Window pastWeek2 = Windows.of(
     *          new Document("range", Arrays.asList(-1, "current"))
     *                  .append("unit", MongoTimeUnit.WEEK.value()));
     * }</pre>
     *
     * @param window A {@link Bson} representing the required window.
     * @return The constructed window.
     */
    public static Window of(final Bson window) {
        return new BsonWindow(notNull("window", window));
    }

    /**
     * Creates a documents window whose bounds are determined by a number of documents before and after the current document.
     *
     * @param lower A value based on which the lower bound of the window is calculated.
     * @param upper A value based on which the upper bound of the window is calculated.
     * @return The constructed documents window.
     */
    public static Window documents(final int lower, final int upper) {
        isTrueArgument("lower <= upper", lower <= upper);
        return new SimpleWindow<>(SimpleWindow.TYPE_POSITION_BASED, lower, upper, null);
    }

    /**
     * Creates a documents window whose bounds are determined by a number of documents before and after the current document.
     *
     * @param lower A value based on which the lower bound of the window is calculated.
     * @param upper A value based on which the upper bound of the window is calculated.
     * @return The constructed documents window.
     */
    public static Window documents(final Bound lower, final int upper) {
        notNull("lower", lower);
        if (lower == Bound.CURRENT) {
            isTrueArgument("lower <= upper", upper >= 0);
        }
        return new SimpleWindow<>(SimpleWindow.TYPE_POSITION_BASED, lower.value(), upper, null);
    }

    /**
     * Creates a documents window whose bounds are determined by a number of documents before and after the current document.
     *
     * @param lower A value based on which the lower bound of the window is calculated.
     * @param upper A value based on which the upper bound of the window is calculated.
     * @return The constructed documents window.
     */
    public static Window documents(final int lower, final Bound upper) {
        notNull("upper", upper);
        if (upper == Bound.CURRENT) {
            isTrueArgument("lower <= upper", lower <= 0);
        }
        return new SimpleWindow<>(SimpleWindow.TYPE_POSITION_BASED, lower, upper.value(), null);
    }

    /**
     * Creates a documents window whose bounds are determined by a number of documents before and after the current document.
     *
     * @param lower A value based on which the lower bound of the window is calculated.
     * @param upper A value based on which the upper bound of the window is calculated.
     * @return The constructed documents window.
     */
    public static Window documents(final Bound lower, final Bound upper) {
        notNull("lower", lower);
        notNull("upper", upper);
        return new SimpleWindow<>(SimpleWindow.TYPE_POSITION_BASED, lower.value(), upper.value(), null);
    }

    /**
     * Creates a dynamically-sized range window whose bounds are determined by a range of possible values around
     * the value of the {@link Aggregates#setWindowFields(Object, Bson, Iterable) sortBy} field in the current document.
     *
     * @param lower A value based on which the lower bound of the window is calculated.
     * @param upper A value based on which the upper bound of the window is calculated.
     * @return The constructed range window.
     */
    public static Window range(final long lower, final long upper) {
        isTrueArgument("lower <= upper", lower <= upper);
        return new SimpleWindow<>(SimpleWindow.TYPE_RANGE_BASED, lower, upper, null);
    }

    /**
     * Creates a dynamically-sized range window whose bounds are determined by a range of possible values around
     * the value of the {@link Aggregates#setWindowFields(Object, Bson, Iterable) sortBy} field in the current document.
     *
     * @param lower A value based on which the lower bound of the window is calculated.
     * @param upper A value based on which the upper bound of the window is calculated.
     * @return The constructed range window.
     */
    public static Window range(final double lower, final double upper) {
        isTrueArgument("lower <= upper", lower <= upper);
        return new SimpleWindow<>(SimpleWindow.TYPE_RANGE_BASED, lower, upper, null);
    }

    /**
     * Creates a dynamically-sized range window whose bounds are determined by a range of possible values around
     * the value of the {@link Aggregates#setWindowFields(Object, Bson, Iterable) sortBy} field in the current document.
     *
     * @param lower A value based on which the lower bound of the window is calculated.
     * @param upper A value based on which the upper bound of the window is calculated.
     * @return The constructed range window.
     */
    public static Window range(final Decimal128 lower, final Decimal128 upper) {
        notNull("lower", lower);
        notNull("upper", upper);
        isTrueArgument("lower <= upper", lower.compareTo(upper) <= 0);
        return new SimpleWindow<>(SimpleWindow.TYPE_RANGE_BASED, lower, upper, null);
    }

    /**
     * Creates a dynamically-sized range window whose bounds are determined by a range of possible values around
     * the value of the {@link Aggregates#setWindowFields(Object, Bson, Iterable) sortBy} field in the current document.
     *
     * @param lower A value based on which the lower bound of the window is calculated.
     * @param upper A value based on which the upper bound of the window is calculated.
     * @return The constructed range window.
     */
    public static Window range(final Bound lower, final long upper) {
        notNull("lower", lower);
        return new SimpleWindow<>(SimpleWindow.TYPE_RANGE_BASED, lower.value(), upper, null);
    }

    /**
     * Creates a dynamically-sized range window whose bounds are determined by a range of possible values around
     * the value of the {@link Aggregates#setWindowFields(Object, Bson, Iterable) sortBy} field in the current document.
     *
     * @param lower A value based on which the lower bound of the window is calculated.
     * @param upper A value based on which the upper bound of the window is calculated.
     * @return The constructed range window.
     */
    public static Window range(final Bound lower, final double upper) {
        notNull("lower", lower);
        return new SimpleWindow<>(SimpleWindow.TYPE_RANGE_BASED, lower.value(), upper, null);
    }

    /**
     * Creates a dynamically-sized range window whose bounds are determined by a range of possible values around
     * the value of the {@link Aggregates#setWindowFields(Object, Bson, Iterable) sortBy} field in the current document.
     *
     * @param lower A value based on which the lower bound of the window is calculated.
     * @param upper A value based on which the upper bound of the window is calculated.
     * @return The constructed range window.
     */
    public static Window range(final Bound lower, final Decimal128 upper) {
        notNull("lower", lower);
        notNull("upper", upper);
        return new SimpleWindow<>(SimpleWindow.TYPE_RANGE_BASED, lower.value(), upper, null);
    }

    /**
     * Creates a dynamically-sized range window whose bounds are determined by a range of possible values around
     * the value of the {@link Aggregates#setWindowFields(Object, Bson, Iterable) sortBy} field in the current document.
     *
     * @param lower A value based on which the lower bound of the window is calculated.
     * @param upper A value based on which the upper bound of the window is calculated.
     * @return The constructed range window.
     */
    public static Window range(final long lower, final Bound upper) {
        notNull("upper", upper);
        return new SimpleWindow<>(SimpleWindow.TYPE_RANGE_BASED, lower, upper.value(), null);
    }

    /**
     * Creates a dynamically-sized range window whose bounds are determined by a range of possible values around
     * the value of the {@link Aggregates#setWindowFields(Object, Bson, Iterable) sortBy} field in the current document.
     *
     * @param lower A value based on which the lower bound of the window is calculated.
     * @param upper A value based on which the upper bound of the window is calculated.
     * @return The constructed range window.
     */
    public static Window range(final double lower, final Bound upper) {
        notNull("upper", upper);
        return new SimpleWindow<>(SimpleWindow.TYPE_RANGE_BASED, lower, upper.value(), null);
    }

    /**
     * Creates a dynamically-sized range window whose bounds are determined by a range of possible values around
     * the value of the {@link Aggregates#setWindowFields(Object, Bson, Iterable) sortBy} field in the current document.
     *
     * @param lower A value based on which the lower bound of the window is calculated.
     * @param upper A value based on which the upper bound of the window is calculated.
     * @return The constructed range window.
     */
    public static Window range(final Decimal128 lower, final Bound upper) {
        notNull("lower", lower);
        notNull("upper", upper);
        return new SimpleWindow<>(SimpleWindow.TYPE_RANGE_BASED, lower, upper.value(), null);
    }

    /**
     * Creates a dynamically-sized range window whose bounds are determined by a range of possible values around
     * the BSON {@link BsonType#DATE_TIME Date} value of the {@link Aggregates#setWindowFields(Object, Bson, Iterable) sortBy}
     * field in the current document.
     *
     * @param lower A value based on which the lower bound of the window is calculated.
     * @param upper A value based on which the upper bound of the window is calculated.
     * @param unit A time unit in which {@code lower} and {@code upper} are specified.
     * @return The constructed range window.
     */
    public static Window timeRange(final long lower, final long upper, final MongoTimeUnit unit) {
        notNull("unit", unit);
        isTrueArgument("lower <= upper", lower <= upper);
        return new SimpleWindow<>(SimpleWindow.TYPE_RANGE_BASED, lower, upper, unit);
    }

    /**
     * Creates a dynamically-sized range window whose bounds are determined by a range of possible values around
     * the BSON {@link BsonType#DATE_TIME Date} value of the {@link Aggregates#setWindowFields(Object, Bson, Iterable) sortBy}
     * field in the current document.
     *
     * @param lower A value based on which the lower bound of the window is calculated.
     * @param upper A value based on which the upper bound of the window is calculated.
     * @param unit A time unit in which {@code upper} is specified.
     * @return The constructed range window.
     */
    public static Window timeRange(final Bound lower, final long upper, final MongoTimeUnit unit) {
        notNull("lower", lower);
        notNull("unit", unit);
        return new SimpleWindow<>(SimpleWindow.TYPE_RANGE_BASED, lower.value(), upper, unit);
    }

    /**
     * Creates a dynamically-sized range window whose bounds are determined by a range of possible values around
     * the BSON {@link BsonType#DATE_TIME Date} value of the {@link Aggregates#setWindowFields(Object, Bson, Iterable) sortBy}
     * field in the current document.
     *
     * @param lower A value based on which the lower bound of the window is calculated.
     * @param unit A time unit in which {@code lower} is specified.
     * @param upper A value based on which the upper bound of the window is calculated.
     * @return The constructed range window.
     */
    public static Window timeRange(final long lower, final MongoTimeUnit unit, final Bound upper) {
        notNull("unit", unit);
        notNull("upper", upper);
        return new SimpleWindow<>(SimpleWindow.TYPE_RANGE_BASED, lower, upper.value(), unit);
    }

    private Windows() {
        throw new UnsupportedOperationException();
    }

    /**
     * Special values that may be used when specifying the bounds of a {@linkplain Window window}.
     *
     * @mongodb.server.release 5.0
     * @since 4.3
     */
    public enum Bound {
        /**
         * The {@linkplain Window window} bound is determined by the current document and is inclusive.
         */
        CURRENT("current"),
        /**
         * The {@linkplain Window window} bound is the same as the corresponding bound of the partition encompassing it.
         */
        UNBOUNDED("unbounded");

        private final String value;

        Bound(final String value) {
            this.value = value;
        }

        String value() {
            return value;
        }
    }

    private static class SimpleWindow<L, U> implements Window {
        static final String TYPE_POSITION_BASED = "documents";
        static final String TYPE_RANGE_BASED = "range";

        private final String type;
        private final L lower;
        private final U upper;
        @Nullable
        private final MongoTimeUnit unit;

        SimpleWindow(final String type, final L lower, final U upper, @Nullable final MongoTimeUnit unit) {
            this.lower = lower;
            this.upper = upper;
            this.type = type;
            this.unit = unit;
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> tDocumentClass, final CodecRegistry codecRegistry) {
            BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());
            writer.writeStartDocument();
            writer.writeStartArray(type);
            BuildersHelper.encodeValue(writer, lower, codecRegistry);
            BuildersHelper.encodeValue(writer, upper, codecRegistry);
            writer.writeEndArray();
            if (unit != null) {
                writer.writeString("unit", unit.value());
            }
            writer.writeEndDocument();
            return writer.getDocument();
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final SimpleWindow<?, ?> that = (SimpleWindow<?, ?>) o;
            return type.equals(that.type) && lower.equals(that.lower) && upper.equals(that.upper) && unit == that.unit;
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, lower, upper, unit);
        }

        @Override
        public String toString() {
            return "Window{"
                    + "type=" + type
                    + ", lower=" + lower
                    + ", upper=" + upper
                    + ", unit=" + unit
                    + '}';
        }
    }

    private static final class BsonWindow implements Window {
        private final Bson wrapped;

        BsonWindow(final Bson document) {
            wrapped = assertNotNull(document);
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> tDocumentClass, final CodecRegistry codecRegistry) {
            return wrapped.toBsonDocument(tDocumentClass, codecRegistry);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final BsonWindow that = (BsonWindow) o;
            return wrapped.equals(that.wrapped);
        }

        @Override
        public int hashCode() {
            return Objects.hash(wrapped);
        }

        @Override
        public String toString() {
            return wrapped.toString();
        }
    }
}
