/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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

package com.google.code.morphia.query;

import com.google.code.morphia.logging.Logr;
import com.google.code.morphia.logging.MorphiaLoggerFactory;
import com.google.code.morphia.utils.Assert;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class FieldEndImpl<T extends CriteriaContainerImpl> implements FieldEnd<T> {
    private static final Logr log = MorphiaLoggerFactory.get(FieldEndImpl.class);

    private final QueryImpl<?> query;
    private final String field;
    private final T target;
    private boolean not;
    private final boolean validateName;

    private FieldEndImpl(final QueryImpl<?> query, final String field, final T target, final boolean validateName,
                         final boolean not) {
        this.query = query;
        this.field = field;
        this.target = target;
        this.validateName = validateName;
        this.not = not;
    }

    public FieldEndImpl(final QueryImpl<?> query, final String field, final T target, final boolean validateName) {

        this(query, field, target, validateName, false);
    }

    /**
     * Add a criteria
     */
    private T addCrit(final FilterOperator op, final Object val) {
        target.add(new FieldCriteria(query, field, op, val, validateName, query.isValidatingTypes(), not));
        return target;
    }

    private T addGeoCrit(final FilterOperator op, final Object val, final Map<String, Object> opts) {
        if (not) {
            throw new QueryException("Geospatial queries cannot be negated with 'not'.");
        }

        target.add(new GeoFieldCriteria(query, field, op, val, validateName, false, opts));
        return target;
    }

    public FieldEnd<T> not() {
        not = !not;
        return this;
    }

    public T startsWith(final String prefix) {
        Assert.parametersNotNull("val", prefix);
        return addCrit(FilterOperator.EQUAL, Pattern.compile("^" + prefix));
    }

    public T startsWithIgnoreCase(final String prefix) {
        Assert.parametersNotNull("val", prefix);
        return addCrit(FilterOperator.EQUAL, Pattern.compile("^" + prefix, Pattern.CASE_INSENSITIVE));
    }

    public T endsWith(final String suffix) {
        Assert.parametersNotNull("val", suffix);
        return addCrit(FilterOperator.EQUAL, Pattern.compile(suffix + "$"));
    }

    public T endsWithIgnoreCase(final String suffix) {
        Assert.parametersNotNull("val", suffix);
        return addCrit(FilterOperator.EQUAL, Pattern.compile(suffix + "$", Pattern.CASE_INSENSITIVE));
    }

    public T contains(final String string) {
        Assert.parametersNotNull("val", string);
        return addCrit(FilterOperator.EQUAL, Pattern.compile(string));
    }

    public T containsIgnoreCase(final String string) {
        Assert.parametersNotNull("val", string);
        return addCrit(FilterOperator.EQUAL, Pattern.compile(string, Pattern.CASE_INSENSITIVE));
    }

    public T exists() {
        return addCrit(FilterOperator.EXISTS, true);
    }

    public T doesNotExist() {
        return addCrit(FilterOperator.EXISTS, false);
    }

    public T equal(final Object val) {
        return addCrit(FilterOperator.EQUAL, val);
    }

    public T greaterThan(final Object val) {
        Assert.parametersNotNull("val", val);
        return addCrit(FilterOperator.GREATER_THAN, val);
    }

    public T greaterThanOrEq(final Object val) {
        Assert.parametersNotNull("val", val);
        return addCrit(FilterOperator.GREATER_THAN_OR_EQUAL, val);
    }

    public T hasThisOne(final Object val) {
        return addCrit(FilterOperator.EQUAL, val);
    }

    public T hasAllOf(final Iterable<?> vals) {
        Assert.parametersNotNull("vals", vals);
        Assert.parameterNotEmpty(vals, "vals");
        return addCrit(FilterOperator.ALL, vals);
    }

    public T hasAnyOf(final Iterable<?> vals) {
        Assert.parametersNotNull("vals", vals);
//		Assert.parameterNotEmpty(vals,"vals"); //it is valid but will never return any results.
        if (log.isWarningEnabled()) {
            if (!vals.iterator().hasNext()) {
                log.warning("Specified an empty list/collection with the '" + field + "' criteria");
            }
        }
        return addCrit(FilterOperator.IN, vals);
    }

    public T in(final Iterable<?> vals) {
        return this.hasAnyOf(vals);
    }

    public T hasThisElement(final Object val) {
        Assert.parametersNotNull("val", val);
        return addCrit(FilterOperator.ELEMENT_MATCH, val);
    }

    public T hasNoneOf(final Iterable<?> vals) {
        Assert.parametersNotNull("vals", vals);
        Assert.parameterNotEmpty(vals, "vals");
        return addCrit(FilterOperator.NOT_IN, vals);
    }

    public T notIn(final Iterable<?> vals) {
        return this.hasNoneOf(vals);
    }

    public T lessThan(final Object val) {
        Assert.parametersNotNull("val", val);
        return addCrit(FilterOperator.LESS_THAN, val);
    }

    public T lessThanOrEq(final Object val) {
        Assert.parametersNotNull("val", val);
        return addCrit(FilterOperator.LESS_THAN_OR_EQUAL, val);
    }

    public T notEqual(final Object val) {
        return addCrit(FilterOperator.NOT_EQUAL, val);
    }

    public T sizeEq(final int val) {
        return addCrit(FilterOperator.SIZE, val);
    }

    public T near(final double x, final double y) {
        return near(x, y, false);
    }

    public T near(final double x, final double y, final double radius) {
        return near(x, y, radius, false);
    }

    public T near(final double x, final double y, final double radius, final boolean spherical) {
        return addGeoCrit(spherical ? FilterOperator.NEAR_SPHERE : FilterOperator.NEAR, new double[]{x, y},
                          opts("$maxDistance", radius));
    }

    public T near(final double x, final double y, final boolean spherical) {
        return addGeoCrit(spherical ? FilterOperator.NEAR_SPHERE : FilterOperator.NEAR, new double[]{x, y}, null);
    }

    public T within(final double x, final double y, final double radius) {
        return within(x, y, radius, false);
    }

    public T within(final double x, final double y, final double radius, final boolean spherical) {
        return addGeoCrit(spherical ? FilterOperator.WITHIN_CIRCLE_SPHERE : FilterOperator.WITHIN_CIRCLE,
                          new Object[]{new double[]{x, y}, radius}, null);
    }

    public T within(final double x1, final double y1, final double x2, final double y2) {
        return addGeoCrit(FilterOperator.WITHIN_BOX, new double[][]{new double[]{x1, y1}, new double[]{x2, y2}}, null);
    }

    private Map<String, Object> opts(final String s, final Object v) {
        final Map<String, Object> opts = new HashMap<String, Object>();
        opts.put(s, v);
        return opts;
    }

    private Map<String, Object> opts(final String s1, final Object v1, final String s2, final Object v2) {
        final Map<String, Object> opts = new HashMap<String, Object>();
        opts.put(s1, v1);
        opts.put(s2, v2);
        return opts;
    }

}
