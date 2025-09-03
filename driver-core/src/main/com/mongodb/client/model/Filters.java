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

import com.mongodb.client.model.geojson.Geometry;
import com.mongodb.client.model.geojson.Point;
import com.mongodb.client.model.search.SearchCollector;
import com.mongodb.client.model.search.SearchOperator;
import com.mongodb.client.model.search.SearchOptions;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWriter;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonRegularExpression;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.client.model.BuildersHelper.encodeValue;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableSet;

/**
 * A factory for query filters. A convenient way to use this class is to statically import all of its methods, which allows usage like:
 * <blockquote><pre>
 *    collection.find(and(eq("x", 1), lt("y", 3)));
 * </pre></blockquote>
 * @since 3.0
 */
public final class Filters {

    private Filters() {
    }

    /**
     * Creates a filter that matches all documents where the value of _id field equals the specified value. Note that this doesn't
     * actually generate a $eq operator, as the query language doesn't require it.
     *
     * @param value     the value, which may be null
     * @param <TItem>   the value type
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/eq $eq
     *
     * @since 3.4
     */
    public static <TItem> Bson eq(@Nullable final TItem value) {
        return eq("_id", value);
    }

    /**
     * Creates a filter that matches all documents where the value of the field name equals the specified value. Note that this doesn't
     * actually generate a $eq operator, as the query language doesn't require it.
     *
     * @param fieldName the field name
     * @param value     the value, which may be null
     * @param <TItem>   the value type
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/eq $eq
     */
    public static <TItem> Bson eq(final String fieldName, @Nullable final TItem value) {
        return new SimpleEncodingFilter<>(fieldName, value);
    }

    /**
     * Creates a filter that matches all documents where the value of the field name does not equal the specified value.
     *
     * @param fieldName the field name
     * @param value     the value, which may be null
     * @param <TItem>   the value type
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/ne $ne
     */
    public static <TItem> Bson ne(final String fieldName, @Nullable final TItem value) {
        return new OperatorFilter<>("$ne", fieldName, value);
    }

    /**
     * Creates a filter that matches all documents where the value of the given field is greater than the specified value.
     *
     * @param fieldName the field name
     * @param value     the value
     * @param <TItem>   the value type
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/gt $gt
     */
    public static <TItem> Bson gt(final String fieldName, final TItem value) {
        return new OperatorFilter<>("$gt", fieldName, value);
    }

    /**
     * Creates a filter that matches all documents where the value of the given field is less than the specified value.
     *
     * @param fieldName the field name
     * @param value     the value
     * @param <TItem>   the value type
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/lt $lt
     */
    public static <TItem> Bson lt(final String fieldName, final TItem value) {
        return new OperatorFilter<>("$lt", fieldName, value);
    }

    /**
     * Creates a filter that matches all documents where the value of the given field is greater than or equal to the specified value.
     *
     * @param fieldName the field name
     * @param value     the value
     * @param <TItem>   the value type
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/gte $gte
     */
    public static <TItem> Bson gte(final String fieldName, final TItem value) {
        return new OperatorFilter<>("$gte", fieldName, value);
    }

    /**
     * Creates a filter that matches all documents where the value of the given field is less than or equal to the specified value.
     *
     * @param fieldName the field name
     * @param value     the value
     * @param <TItem>   the value type
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/lte $lte
     */
    public static <TItem> Bson lte(final String fieldName, final TItem value) {
        return new OperatorFilter<>("$lte", fieldName, value);
    }

    /**
     * Creates a filter that matches all documents where the value of a field equals any value in the list of specified values.
     *
     * @param fieldName the field name
     * @param values    the list of values
     * @param <TItem>   the value type
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/in $in
     */
    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <TItem> Bson in(final String fieldName, final TItem... values) {
        return in(fieldName, asList(values));
    }

    /**
     * Creates a filter that matches all documents where the value of a field equals any value in the list of specified values.
     *
     * @param fieldName the field name
     * @param values    the list of values
     * @param <TItem>   the value type
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/in $in
     */
    public static <TItem> Bson in(final String fieldName, final Iterable<TItem> values) {
        return new IterableOperatorFilter<>(fieldName, "$in", values);
    }

    /**
     * Creates a filter that matches all documents where the value of a field does not equal any of the specified values or does not exist.
     *
     * @param fieldName the field name
     * @param values    the list of values
     * @param <TItem>   the value type
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/nin $nin
     */
    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <TItem> Bson nin(final String fieldName, final TItem... values) {
        return nin(fieldName, asList(values));
    }

    /**
     * Creates a filter that matches all documents where the value of a field does not equal any of the specified values or does not exist.
     *
     * @param fieldName the field name
     * @param values    the list of values
     * @param <TItem>   the value type
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/nin $nin
     */
    public static <TItem> Bson nin(final String fieldName, final Iterable<TItem> values) {
        return new IterableOperatorFilter<>(fieldName, "$nin", values);
    }

    /**
     * Creates a filter that performs a logical AND of the provided list of filters.
     *
     * <blockquote><pre>
     *    and(eq("x", 1), lt("y", 3))
     * </pre></blockquote>
     *
     * will generate a MongoDB query like:
     * <blockquote><pre>
     *    { $and: [{x : 1}, {y : {$lt : 3}}]}
     * </pre></blockquote>
     *
     * @param filters the list of filters to and together
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/and $and
     */
    public static Bson and(final Iterable<Bson> filters) {
        return new AndFilter(filters);
    }

    /**
     * Creates a filter that performs a logical AND of the provided list of filters.
     *
     * <blockquote><pre>
     *    and(eq("x", 1), lt("y", 3))
     * </pre></blockquote>
     *
     * will generate a MongoDB query like:
     * <blockquote><pre>
     *    { $and: [{x : 1}, {y : {$lt : 3}}]}
     * </pre></blockquote>
     *
     * @param filters the list of filters to and together
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/and $and
     */
    public static Bson and(final Bson... filters) {
        return and(asList(filters));
    }

    /**
     * Creates a filter that preforms a logical OR of the provided list of filters.
     *
     * @param filters the list of filters to and together
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/or $or
     */
    public static Bson or(final Iterable<Bson> filters) {
        return new OrNorFilter(OrNorFilter.Operator.OR, filters);
    }

    /**
     * Creates a filter that preforms a logical OR of the provided list of filters.
     *
     * @param filters the list of filters to and together
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/or $or
     */
    public static Bson or(final Bson... filters) {
        return or(asList(filters));
    }

    /**
     * Creates a filter that matches all documents that do not match the passed in filter.
     * Requires the field name to passed as part of the value passed in and lifts it to create a valid "$not" query:
     *
     * <blockquote><pre>
     *    not(eq("x", 1))
     * </pre></blockquote>
     *
     * will generate a MongoDB query like:
     * <blockquote><pre>
     *    {x : $not: {$eq : 1}}
     * </pre></blockquote>
     *
     * @param filter the value
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/not $not
     */
    public static Bson not(final Bson filter) {
        return new NotFilter(filter);
    }

    /**
     * Creates a filter that performs a logical NOR operation on all the specified filters.
     *
     * @param filters the list of values
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/nor $nor
     */
    public static Bson nor(final Bson... filters) {
        return nor(asList(filters));
    }

    /**
     * Creates a filter that performs a logical NOR operation on all the specified filters.
     *
     * @param filters the list of values
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/nor $nor
     */
    public static Bson nor(final Iterable<Bson> filters) {
        return new OrNorFilter(OrNorFilter.Operator.NOR, filters);
    }

    /**
     * Creates a filter that matches all documents that contain the given field.
     *
     * @param fieldName the field name
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/exists $exists
     */
    public static Bson exists(final String fieldName) {
        return exists(fieldName, true);
    }

    /**
     * Creates a filter that matches all documents that either contain or do not contain the given field, depending on the value of the
     * exists parameter.
     *
     * @param fieldName the field name
     * @param exists    true to check for existence, false to check for absence
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/exists $exists
     */

    public static Bson exists(final String fieldName, final boolean exists) {
        return new OperatorFilter<>("$exists", fieldName, BsonBoolean.valueOf(exists));
    }

    /**
     * Creates a filter that matches all documents where the value of the field is of the specified BSON type.
     *
     * @param fieldName the field name
     * @param type      the BSON type
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/type $type
     */
    public static Bson type(final String fieldName, final BsonType type) {
        return new OperatorFilter<>("$type", fieldName, new BsonInt32(type.getValue()));
    }

    /**
     * Creates a filter that matches all documents where the value of the field is of the specified BSON type.
     *
     * @param fieldName the field name
     * @param type      the string representation of the BSON type
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/type $type
     */
    public static Bson type(final String fieldName, final String type) {
        return new OperatorFilter<>("$type", fieldName, new BsonString(type));
    }

    /**
     * Creates a filter that matches all documents where the value of a field divided by a divisor has the specified remainder (i.e. perform
     * a modulo operation to select documents).
     *
     * @param fieldName the field name
     * @param divisor   the modulus
     * @param remainder the remainder
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/mod $mod
     */
    public static Bson mod(final String fieldName, final long divisor, final long remainder) {
        return new OperatorFilter<>("$mod", fieldName, new BsonArray(asList(new BsonInt64(divisor), new BsonInt64(remainder))));
    }

    /**
     * Creates a filter that matches all documents where the value of the field matches the given regular expression pattern.
     *
     * @param fieldName the field name
     * @param pattern   the pattern
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/regex $regex
     */
    public static Bson regex(final String fieldName, final String pattern) {
        return regex(fieldName, pattern, null);
    }

    /**
     * Creates a filter that matches all documents where the value of the field matches the given regular expression pattern with the given
     * options applied.
     *
     * @param fieldName the field name
     * @param pattern   the pattern
     * @param options   the options
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/regex $regex
     */
    public static Bson regex(final String fieldName, final String pattern, @Nullable final String options) {
        notNull("pattern", pattern);
        return new SimpleFilter(fieldName, new BsonRegularExpression(pattern, options));
    }

    /**
     * Creates a filter that matches all documents where the value of the field matches the given regular expression pattern.
     *
     * @param fieldName the field name
     * @param pattern   the pattern
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/regex $regex
     */
    public static Bson regex(final String fieldName, final Pattern pattern) {
        notNull("pattern", pattern);
        return new SimpleEncodingFilter<>(fieldName, pattern);
    }

    /**
     * Creates a filter that matches all documents matching the given search term.
     * You may use {@link Projections#metaTextScore(String)} to extract the relevance score assigned to each matched document.
     * <p>
     * {@link Aggregates#search(SearchOperator, SearchOptions)} / {@link Aggregates#search(SearchCollector, SearchOptions)}
     * is a more powerful full-text search alternative.</p>
     *
     * @param search the search term
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/text $text
     */
    public static Bson text(final String search) {
        notNull("search", search);
        return text(search, new TextSearchOptions());
    }

    /**
     * Creates a filter that matches all documents matching the given the search term with the given text search options.
     * You may use {@link Projections#metaTextScore(String)} to extract the relevance score assigned to each matched document.
     * <p>
     * {@link Aggregates#search(SearchOperator, SearchOptions)} / {@link Aggregates#search(SearchCollector, SearchOptions)}
     * is a more powerful full-text search alternative.</p>
     *
     * @param search            the search term
     * @param textSearchOptions the text search options to use
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/text $text
     * @since 3.2
     */
    public static Bson text(final String search, final TextSearchOptions textSearchOptions) {
        notNull("search", search);
        notNull("textSearchOptions", textSearchOptions);
        return new TextFilter(search, textSearchOptions);
    }

    /**
     * Creates a filter that matches all documents for which the given expression is true.
     *
     * @param javaScriptExpression the JavaScript expression
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/where $where
     */
    public static Bson where(final String javaScriptExpression) {
        notNull("javaScriptExpression", javaScriptExpression);
        return new BsonDocument("$where", new BsonString(javaScriptExpression));
    }

    /**
     * Allows the use of aggregation expressions within the query language.
     *
     * @param expression the aggregation expression
     * @param <TExpression> the expression type
     * @return the filter
     * @since 3.6
     * @mongodb.server.release 3.6
     * @mongodb.driver.manual reference/operator/query/expr/ $expr
     */
    public static <TExpression> Bson expr(final TExpression expression) {
        return new SimpleEncodingFilter<>("$expr", expression);
    }

    /**
     * Creates a filter that matches all documents where the value of a field is an array that contains all the specified values.
     *
     * @param fieldName the field name
     * @param values    the list of values
     * @param <TItem>   the value type
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/all $all
     */
    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <TItem> Bson all(final String fieldName, final TItem... values) {
        return all(fieldName, asList(values));
    }

    /**
     * Creates a filter that matches all documents where the value of a field is an array that contains all the specified values.
     *
     * @param fieldName the field name
     * @param values    the list of values
     * @param <TItem>   the value type
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/all $all
     */
    public static <TItem> Bson all(final String fieldName, final Iterable<TItem> values) {
        return new IterableOperatorFilter<>(fieldName, "$all", values);
    }

    /**
     * Creates a filter that matches all documents containing a field that is an array where at least one member of the array matches the
     * given filter.
     *
     * @param fieldName the field name
     * @param filter    the filter to apply to each element
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/elemMatch $elemMatch
     */
    public static Bson elemMatch(final String fieldName, final Bson filter) {
        return new Bson() {
            @Override
            public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
                return new BsonDocument(fieldName, new BsonDocument("$elemMatch", filter.toBsonDocument(documentClass, codecRegistry)));
            }
        };
    }

    /**
     * Creates a filter that matches all documents where the value of a field is an array of the specified size.
     *
     * @param fieldName the field name
     * @param size      the size of the array
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/size $size
     */
    public static Bson size(final String fieldName, final int size) {
        return new OperatorFilter<>("$size", fieldName, size);
    }

    /**
     * Creates a filter that matches all documents where all of the bit positions are clear in the field.
     *
     * @param fieldName the field name
     * @param bitmask   the bitmask
     * @return the filter
     * @mongodb.server.release 3.2
     * @mongodb.driver.manual reference/operator/query/bitsAllClear $bitsAllClear
     * @since 3.2
     */
    public static Bson bitsAllClear(final String fieldName, final long bitmask) {
        return new OperatorFilter<>("$bitsAllClear", fieldName, bitmask);
    }

    /**
     * Creates a filter that matches all documents where all of the bit positions are set in the field.
     *
     * @param fieldName the field name
     * @param bitmask   the bitmask
     * @return the filter
     * @mongodb.server.release 3.2
     * @mongodb.driver.manual reference/operator/query/bitsAllSet $bitsAllSet
     * @since 3.2
     */
    public static Bson bitsAllSet(final String fieldName, final long bitmask) {
        return new OperatorFilter<>("$bitsAllSet", fieldName, bitmask);
    }

    /**
     * Creates a filter that matches all documents where any of the bit positions are clear in the field.
     *
     * @param fieldName the field name
     * @param bitmask   the bitmask
     * @return the filter
     * @mongodb.server.release 3.2
     * @mongodb.driver.manual reference/operator/query/bitsAllClear $bitsAllClear
     * @since 3.2
     */
    public static Bson bitsAnyClear(final String fieldName, final long bitmask) {
        return new OperatorFilter<>("$bitsAnyClear", fieldName, bitmask);
    }

    /**
     * Creates a filter that matches all documents where any of the bit positions are set in the field.
     *
     * @param fieldName the field name
     * @param bitmask   the bitmask
     * @return the filter
     * @mongodb.server.release 3.2
     * @mongodb.driver.manual reference/operator/query/bitsAnySet $bitsAnySet
     * @since 3.2
     */
    public static Bson bitsAnySet(final String fieldName, final long bitmask) {
        return new OperatorFilter<>("$bitsAnySet", fieldName, bitmask);
    }

    /**
     * Creates a filter that matches all documents containing a field with geospatial data that exists entirely within the specified shape.
     *
     * @param fieldName the field name
     * @param geometry  the bounding GeoJSON geometry object
     * @return the filter
     * @since 3.1
     * @mongodb.driver.manual reference/operator/query/geoWithin/ $geoWithin
     * @mongodb.server.release 2.4
     */
    public static Bson geoWithin(final String fieldName, final Geometry geometry) {
        return new GeometryOperatorFilter<>("$geoWithin", fieldName, geometry);
    }

    /**
     * Creates a filter that matches all documents containing a field with geospatial data that exists entirely within the specified shape.
     *
     * @param fieldName the field name
     * @param geometry  the bounding GeoJSON geometry object
     * @return the filter
     * @since 3.1
     * @mongodb.driver.manual reference/operator/query/geoWithin/ $geoWithin
     * @mongodb.server.release 2.4
     */
    public static Bson geoWithin(final String fieldName, final Bson geometry) {
        return new GeometryOperatorFilter<>("$geoWithin", fieldName, geometry);
    }

    /**
     * Creates a filter that matches all documents containing a field with grid coordinates data that exist entirely within the specified
     * box.
     *
     * @param fieldName   the field name
     * @param lowerLeftX  the lower left x coordinate of the box
     * @param lowerLeftY  the lower left y coordinate of the box
     * @param upperRightX the upper left x coordinate of the box
     * @param upperRightY the upper left y coordinate of the box
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/geoWithin/ $geoWithin
     * @mongodb.driver.manual reference/operator/query/box/#op._S_box $box
     * @mongodb.server.release 2.4
     * @since 3.1
     */
    public static Bson geoWithinBox(final String fieldName, final double lowerLeftX, final double lowerLeftY, final double upperRightX,
                                    final double upperRightY) {
        BsonDocument box = new BsonDocument("$box",
                                                   new BsonArray(asList(new BsonArray(asList(new BsonDouble(lowerLeftX),
                                                           new BsonDouble(lowerLeftY))),
                                                           new BsonArray(asList(new BsonDouble(upperRightX),
                                                                   new BsonDouble(upperRightY))))));
        return new OperatorFilter<>("$geoWithin", fieldName, box);
    }

    /**
     * Creates a filter that matches all documents containing a field with grid coordinates data that exist entirely within the specified
     * polygon.
     *
     * @param fieldName the field name
     * @param points    a list of pairs of x, y coordinates.  Any extra dimensions are ignored
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/geoWithin/ $geoWithin
     * @mongodb.driver.manual reference/operator/query/polygon/#op._S_polygon $polygon
     * @mongodb.server.release 2.4
     * @since 3.1
     */
    public static Bson geoWithinPolygon(final String fieldName, final List<List<Double>> points) {
        BsonArray pointsArray = new BsonArray(points.size());
        for (List<Double> point : points) {
            pointsArray.add(new BsonArray(asList(new BsonDouble(point.get(0)), new BsonDouble(point.get(1)))));
        }
        BsonDocument polygon = new BsonDocument("$polygon", pointsArray);
        return new OperatorFilter<>("$geoWithin", fieldName, polygon);
    }

    /**
     * Creates a filter that matches all documents containing a field with grid coordinates data that exist entirely within the specified
     * circle.
     *
     * @param fieldName the field name
     * @param x         the x coordinate of the circle
     * @param y         the y coordinate of the circle
     * @param radius    the radius of the circle, as measured in the units used by the coordinate system
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/geoWithin/ $geoWithin
     * @mongodb.driver.manual reference/operator/query/center/#op._S_center $center
     * @mongodb.server.release 2.4
     * @since 3.1
     */
    public static Bson geoWithinCenter(final String fieldName, final double x, final double y, final double radius) {
        BsonDocument center = new BsonDocument("$center",
                                                      new BsonArray(asList(new BsonArray(asList(new BsonDouble(x),
                                                              new BsonDouble(y))),
                                                              new BsonDouble(radius))));
        return new OperatorFilter<>("$geoWithin", fieldName, center);
    }

    /**
     * Creates a filter that matches all documents containing a field with geospatial data (GeoJSON or legacy coordinate pairs) that exist
     * entirely within the specified circle, using spherical geometry.  If using longitude and latitude, specify longitude first.
     *
     * @param fieldName the field name
     * @param x         the x coordinate of the circle
     * @param y         the y coordinate of the circle
     * @param radius    the radius of the circle, in radians
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/geoWithin/ $geoWithin
     * @mongodb.driver.manual reference/operator/query/centerSphere/#op._S_centerSphere $centerSphere
     * @mongodb.server.release 2.4
     * @since 3.1
     */
    public static Bson geoWithinCenterSphere(final String fieldName, final double x, final double y, final double radius) {
        BsonDocument centerSphere = new BsonDocument("$centerSphere",
                                                            new BsonArray(asList(new BsonArray(asList(new BsonDouble(x),
                                                                    new BsonDouble(y))),
                                                                    new BsonDouble(radius))));
        return new OperatorFilter<>("$geoWithin", fieldName, centerSphere);
    }

    /**
     * Creates a filter that matches all documents containing a field with geospatial data that intersects with the specified shape.
     *
     * @param fieldName the field name
     * @param geometry  the bounding GeoJSON geometry object
     * @return the filter
     * @since 3.1
     * @mongodb.driver.manual reference/operator/query/geoIntersects/ $geoIntersects
     * @mongodb.server.release 2.4
     */
    public static Bson geoIntersects(final String fieldName, final Bson geometry) {
        return new GeometryOperatorFilter<>("$geoIntersects", fieldName, geometry);
    }

    /**
     * Creates a filter that matches all documents containing a field with geospatial data that intersects with the specified shape.
     *
     * @param fieldName the field name
     * @param geometry  the bounding GeoJSON geometry object
     * @return the filter
     * @since 3.1
     * @mongodb.driver.manual reference/operator/query/geoIntersects/ $geoIntersects
     * @mongodb.server.release 2.4
     */
    public static Bson geoIntersects(final String fieldName, final Geometry geometry) {
        return new GeometryOperatorFilter<>("$geoIntersects", fieldName, geometry);
    }

    /**
     * Creates a filter that matches all documents containing a field with geospatial data that is near the specified GeoJSON point.
     *
     * @param fieldName   the field name
     * @param geometry    the bounding GeoJSON geometry object
     * @param maxDistance the maximum distance from the point, in meters. It may be null.
     * @param minDistance the minimum distance from the point, in meters. It may be null.
     * @return the filter
     * @since 3.1
     * @mongodb.driver.manual reference/operator/query/near/ $near
     * @mongodb.server.release 2.4
     */
    public static Bson near(final String fieldName, final Point geometry, @Nullable final Double maxDistance,
                            @Nullable final Double minDistance) {
        return new GeometryOperatorFilter<>("$near", fieldName, geometry, maxDistance, minDistance);
    }

    /**
     * Creates a filter that matches all documents containing a field with geospatial data that is near the specified GeoJSON point.
     *
     * @param fieldName   the field name
     * @param geometry    the bounding GeoJSON geometry object
     * @param maxDistance the maximum distance from the point, in meters. It may be null.
     * @param minDistance the minimum distance from the point, in meters. It may be null.
     * @return the filter
     * @since 3.1
     * @mongodb.driver.manual reference/operator/query/near/ $near
     * @mongodb.server.release 2.4
     */
    public static Bson near(final String fieldName, final Bson geometry, @Nullable final Double maxDistance,
                            @Nullable final Double minDistance) {
        return new GeometryOperatorFilter<>("$near", fieldName, geometry, maxDistance, minDistance);
    }

    /**
     * Creates a filter that matches all documents containing a field with geospatial data that is near the specified point.
     *
     * @param fieldName   the field name
     * @param x           the x coordinate
     * @param y           the y coordinate
     * @param maxDistance the maximum distance from the point, in radians. It may be null.
     * @param minDistance the minimum distance from the point, in radians. It may be null.
     * @return the filter
     * @since 3.1
     * @mongodb.driver.manual reference/operator/query/near/ $near
     * @mongodb.server.release 2.4
     */
    public static Bson near(final String fieldName, final double x, final double y, @Nullable final Double maxDistance,
                            @Nullable final Double minDistance) {
        return createNearFilterDocument(fieldName, x, y, maxDistance, minDistance, "$near");
    }

    /**
     * Creates a filter that matches all documents containing a field with geospatial data that is near the specified GeoJSON point using
     * spherical geometry.
     *
     * @param fieldName   the field name
     * @param geometry    the bounding GeoJSON geometry object
     * @param maxDistance the maximum distance from the point, in meters. It may be null.
     * @param minDistance the minimum distance from the point, in meters. It may be null.
     * @return the filter
     * @since 3.1
     * @mongodb.driver.manual reference/operator/query/near/ $near
     * @mongodb.server.release 2.4
     */
    public static Bson nearSphere(final String fieldName, final Point geometry, @Nullable final Double maxDistance,
                                  @Nullable final Double minDistance) {
        return new GeometryOperatorFilter<>("$nearSphere", fieldName, geometry, maxDistance, minDistance);
    }

    /**
     * Creates a filter that matches all documents containing a field with geospatial data that is near the specified GeoJSON point using
     * spherical geometry.
     *
     * @param fieldName   the field name
     * @param geometry    the bounding GeoJSON geometry object
     * @param maxDistance the maximum distance from the point, in meters. It may be null.
     * @param minDistance the minimum distance from the point, in meters. It may be null.
     * @return the filter
     * @since 3.1
     * @mongodb.driver.manual reference/operator/query/near/ $near
     * @mongodb.server.release 2.4
     */
    public static Bson nearSphere(final String fieldName, final Bson geometry, @Nullable final Double maxDistance,
                                  @Nullable final Double minDistance) {
        return new GeometryOperatorFilter<>("$nearSphere", fieldName, geometry, maxDistance, minDistance);
    }

    /**
     * Creates a filter that matches all documents containing a field with geospatial data that is near the specified point using
     * spherical geometry.
     *
     * @param fieldName   the field name
     * @param x           the x coordinate
     * @param y           the y coordinate
     * @param maxDistance the maximum distance from the point, in radians. It may be null.
     * @param minDistance the minimum distance from the point, in radians. It may be null.
     * @return the filter
     * @since 3.1
     * @mongodb.driver.manual reference/operator/query/near/ $near
     * @mongodb.server.release 2.4
     */
    public static Bson nearSphere(final String fieldName, final double x, final double y, @Nullable final Double maxDistance,
                                  @Nullable final Double minDistance) {
        return createNearFilterDocument(fieldName, x, y, maxDistance, minDistance, "$nearSphere");
    }

    /**
     * Creates a filter that matches all documents that validate against the given JSON schema document.
     *
     * @param schema the JSON schema to validate against
     * @return the filter
     * @since 3.6
     * @mongodb.server.release 3.6
     * @mongodb.driver.manual reference/operator/query/jsonSchema/ $jsonSchema
     */
    public static Bson jsonSchema(final Bson schema) {
        return new SimpleEncodingFilter<>("$jsonSchema", schema);
    }

    /**
     * Creates an empty filter that will match all documents.
     *
     * @return the filter
     * @since 4.2
     */
    public static Bson empty() {
        return new BsonDocument();
    }


    private static Bson createNearFilterDocument(final String fieldName, final double x, final double y, @Nullable final Double maxDistance,
                                                 @Nullable final Double minDistance, final String operator) {
        BsonDocument nearFilter = new BsonDocument(operator, new BsonArray(asList(new BsonDouble(x), new BsonDouble(y))));
        if (maxDistance != null) {
            nearFilter.append("$maxDistance", new BsonDouble(maxDistance));
        }
        if (minDistance != null) {
            nearFilter.append("$minDistance", new BsonDouble(minDistance));
        }
        return new BsonDocument(fieldName, nearFilter);
    }

    private static String operatorFilterToString(final String fieldName, final String operator, final Object value) {
        return "Operator Filter{"
                       + "fieldName='" + fieldName + '\''
                       + ", operator='" + operator + '\''
                       + ", value=" + value
                       + '}';
    }

    private static final class SimpleFilter implements Bson {
        private final String fieldName;
        private final BsonValue value;

        private SimpleFilter(final String fieldName, final BsonValue value) {
            this.fieldName = notNull("fieldName", fieldName);
            this.value = notNull("value", value);
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
            return new BsonDocument(fieldName, value);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            SimpleFilter that = (SimpleFilter) o;

            if (!fieldName.equals(that.fieldName)) {
                return false;
            }
            return value.equals(that.value);
        }

        @Override
        public int hashCode() {
            int result = fieldName.hashCode();
            result = 31 * result + value.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return operatorFilterToString(fieldName, "$eq", value);
        }
    }

    private static final class OperatorFilter<TItem> implements Bson {
        private final String operatorName;
        private final String fieldName;
        private final TItem value;

        OperatorFilter(final String operatorName, final String fieldName, @Nullable final TItem value) {
            this.operatorName = notNull("operatorName", operatorName);
            this.fieldName = notNull("fieldName", fieldName);
            this.value = value;
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
            BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());

            writer.writeStartDocument();
            writer.writeName(fieldName);
            writer.writeStartDocument();
            writer.writeName(operatorName);
            encodeValue(writer, value, codecRegistry);
            writer.writeEndDocument();
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

            OperatorFilter<?> that = (OperatorFilter<?>) o;

            if (!operatorName.equals(that.operatorName)) {
                return false;
            }
            if (!fieldName.equals(that.fieldName)) {
                return false;
            }
            return Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            int result = operatorName.hashCode();
            result = 31 * result + fieldName.hashCode();
            result = 31 * result + (value != null ? value.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return operatorFilterToString(fieldName, operatorName, value);
        }
    }

    private static class AndFilter implements Bson {
        private final Iterable<Bson> filters;

        AndFilter(final Iterable<Bson> filters) {
            this.filters = notNull("filters", filters);
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
            BsonArray clauses = new BsonArray();
            for (Bson filter : filters) {
                clauses.add(filter.toBsonDocument(documentClass, codecRegistry));
            }

            return new BsonDocument("$and", clauses);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            AndFilter andFilter = (AndFilter) o;

            return filters.equals(andFilter.filters);
        }

        @Override
        public int hashCode() {
            return filters.hashCode();
        }

        @Override
        public String toString() {
            return "And Filter{"
                           + "filters=" + filters
                           + '}';
        }
    }

    private static class OrNorFilter implements Bson {
        private enum Operator {
            OR("$or", "Or"),
            NOR("$nor", "Nor");

            private final String name;
            private final String toStringName;

            Operator(final String name, final String toStringName) {
                this.name = name;
                this.toStringName = toStringName;
            }
        }

        private final Operator operator;
        private final Iterable<Bson> filters;

        OrNorFilter(final Operator operator, final Iterable<Bson> filters) {
            this.operator = notNull("operator", operator);
            this.filters = notNull("filters", filters);
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
            BsonDocument orRenderable = new BsonDocument();

            BsonArray filtersArray = new BsonArray();
            for (Bson filter : filters) {
                filtersArray.add(filter.toBsonDocument(documentClass, codecRegistry));
            }

            orRenderable.put(operator.name, filtersArray);

            return orRenderable;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            OrNorFilter that = (OrNorFilter) o;

            if (operator != that.operator) {
                return false;
            }
            return filters.equals(that.filters);
        }

        @Override
        public int hashCode() {
            int result = operator.hashCode();
            result = 31 * result + filters.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return operator.toStringName + " Filter{"
                           + "filters=" + filters
                           + '}';
        }
    }

    private static class IterableOperatorFilter<TItem> implements Bson {
        private final String fieldName;
        private final String operatorName;
        private final Iterable<TItem> values;

        IterableOperatorFilter(final String fieldName, final String operatorName, final Iterable<TItem> values) {
            this.fieldName = notNull("fieldName", fieldName);
            this.operatorName = notNull("operatorName", operatorName);
            this.values = notNull("values", values);
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
            BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());

            writer.writeStartDocument();
            writer.writeName(fieldName);

            writer.writeStartDocument();
            writer.writeName(operatorName);
            writer.writeStartArray();
            for (TItem value : values) {
                encodeValue(writer, value, codecRegistry);
            }
            writer.writeEndArray();
            writer.writeEndDocument();

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

            IterableOperatorFilter<?> that = (IterableOperatorFilter<?>) o;

            if (!fieldName.equals(that.fieldName)) {
                return false;
            }
            if (!operatorName.equals(that.operatorName)) {
                return false;
            }
            return values.equals(that.values);
        }

        @Override
        public int hashCode() {
            int result = fieldName.hashCode();
            result = 31 * result + operatorName.hashCode();
            result = 31 * result + values.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return operatorFilterToString(fieldName, operatorName, values);
        }
    }

    private static class SimpleEncodingFilter<TItem> implements Bson {
        private final String fieldName;
        private final TItem value;

        SimpleEncodingFilter(final String fieldName, @Nullable final TItem value) {
            this.fieldName = notNull("fieldName", fieldName);
            this.value = value;
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
            BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());

            writer.writeStartDocument();
            writer.writeName(fieldName);
            encodeValue(writer, value, codecRegistry);
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

            SimpleEncodingFilter<?> that = (SimpleEncodingFilter<?>) o;

            if (!fieldName.equals(that.fieldName)) {
                return false;
            }
            return Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            int result = fieldName.hashCode();
            result = 31 * result + (value != null ? value.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Filter{"
                           + "fieldName='" + fieldName + '\''
                           + ", value=" + value
                           + '}';
        }
    }

    private static class NotFilter implements Bson {
        private static final Set<String> DBREF_KEYS = unmodifiableSet(new HashSet<>(asList("$ref", "$id")));
        private static final Set<String> DBREF_KEYS_WITH_DB =  unmodifiableSet(new HashSet<>(asList("$ref", "$id", "$db")));
        private final Bson filter;

        NotFilter(final Bson filter) {
            this.filter = notNull("filter", filter);
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
            BsonDocument filterDocument = filter.toBsonDocument(documentClass, codecRegistry);
            if (filterDocument.size() == 1) {
                Map.Entry<String, BsonValue> entry = filterDocument.entrySet().iterator().next();
                return createFilter(entry.getKey(), entry.getValue());
            } else {
                BsonArray values = new BsonArray(filterDocument.size());
                for (Map.Entry<String, BsonValue> docs : filterDocument.entrySet()) {
                    values.add(new BsonDocument(docs.getKey(), docs.getValue()));
                }
                return createFilter("$and", values);
            }
        }

        private boolean containsOperator(final BsonDocument value) {
            Set<String> keys = value.keySet();
            if (keys.equals(DBREF_KEYS) || keys.equals(DBREF_KEYS_WITH_DB)) {
                return false;
            }

            for (String key : keys) {
                if (key.startsWith("$")) {
                    return true;
                }
            }

            return false;
        }

        private BsonDocument createFilter(final String fieldName, final BsonValue value) {
            if (fieldName.startsWith("$")) {
                return new BsonDocument("$not", new BsonDocument(fieldName, value));
            } else if ((value.isDocument() && containsOperator(value.asDocument())) || value.isRegularExpression()) {
                return new BsonDocument(fieldName, new BsonDocument("$not", value));
            }
            return new BsonDocument(fieldName, new BsonDocument("$not", new BsonDocument("$eq", value)));
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            NotFilter notFilter = (NotFilter) o;

            return filter.equals(notFilter.filter);
        }

        @Override
        public int hashCode() {
            return filter.hashCode();
        }

        @Override
        public String toString() {
            return "Not Filter{"
                           + "filter=" + filter
                           + '}';
        }
    }

    private static class GeometryOperatorFilter<TItem> implements Bson {
        private final String operatorName;
        private final String fieldName;
        private final TItem geometry;
        private final Double maxDistance;
        private final Double minDistance;

        GeometryOperatorFilter(final String operatorName, final String fieldName, final TItem geometry) {
            this(operatorName, fieldName, geometry, null, null);
        }

        GeometryOperatorFilter(final String operatorName, final String fieldName, final TItem geometry,
                               @Nullable final Double maxDistance, @Nullable final Double minDistance) {
            this.operatorName = operatorName;
            this.fieldName = notNull("fieldName", fieldName);
            this.geometry = notNull("geometry", geometry);
            this.maxDistance = maxDistance;
            this.minDistance = minDistance;
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
            BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());
            writer.writeStartDocument();
            writer.writeName(fieldName);
            writer.writeStartDocument();
            writer.writeName(operatorName);
            writer.writeStartDocument();
            writer.writeName("$geometry");
            encodeValue(writer, geometry, codecRegistry);
            if (maxDistance != null) {
                writer.writeDouble("$maxDistance", maxDistance);
            }
            if (minDistance != null) {
                writer.writeDouble("$minDistance", minDistance);
            }
            writer.writeEndDocument();
            writer.writeEndDocument();
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

            GeometryOperatorFilter<?> that = (GeometryOperatorFilter<?>) o;

            if (!Objects.equals(operatorName, that.operatorName)) {
                return false;
            }
            if (!fieldName.equals(that.fieldName)) {
                return false;
            }
            if (!geometry.equals(that.geometry)) {
                return false;
            }
            if (!Objects.equals(maxDistance, that.maxDistance)) {
                return false;
            }
            return Objects.equals(minDistance, that.minDistance);
        }

        @Override
        public int hashCode() {
            int result = operatorName != null ? operatorName.hashCode() : 0;
            result = 31 * result + fieldName.hashCode();
            result = 31 * result + geometry.hashCode();
            result = 31 * result + (maxDistance != null ? maxDistance.hashCode() : 0);
            result = 31 * result + (minDistance != null ? minDistance.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Geometry Operator Filter{"
                           + "fieldName='" + fieldName + '\''
                           + ", operator='" + operatorName + '\''
                           + ", geometry=" + geometry
                           + ", maxDistance=" + maxDistance
                           + ", minDistance=" + minDistance
                           + '}';
        }
    }

    private static class TextFilter implements Bson {
        private final String search;
        private final TextSearchOptions textSearchOptions;

        TextFilter(final String search, final TextSearchOptions textSearchOptions) {
            this.search = search;
            this.textSearchOptions = textSearchOptions;
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
            BsonDocument searchDocument = new BsonDocument("$search", new BsonString(search));

            String language = textSearchOptions.getLanguage();
            if (language != null) {
                searchDocument.put("$language", new BsonString(language));
            }

            Boolean caseSensitive = textSearchOptions.getCaseSensitive();
            if (caseSensitive != null) {
                searchDocument.put("$caseSensitive", BsonBoolean.valueOf(caseSensitive));
            }

            Boolean diacriticSensitive = textSearchOptions.getDiacriticSensitive();
            if (diacriticSensitive != null) {
                searchDocument.put("$diacriticSensitive", BsonBoolean.valueOf(diacriticSensitive));
            }
            return new BsonDocument("$text", searchDocument);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            TextFilter that = (TextFilter) o;

            if (!Objects.equals(search, that.search)) {
                return false;
            }
            return Objects.equals(textSearchOptions, that.textSearchOptions);
        }

        @Override
        public int hashCode() {
            int result = search != null ? search.hashCode() : 0;
            result = 31 * result + (textSearchOptions != null ? textSearchOptions.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Text Filter{"
                           + "search='" + search + '\''
                           + ", textSearchOptions=" + textSearchOptions
                           + '}';
        }
    }

}
