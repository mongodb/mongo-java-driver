/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

public interface FieldEnd<T> {
    FieldEnd<T> not();

    T exists();

    T doesNotExist();

    T greaterThan(Object val);

    T greaterThanOrEq(Object val);

    T lessThan(Object val);

    T lessThanOrEq(Object val);

    T equal(Object val);

    T notEqual(Object val);

    T startsWith(String prefix);

    T startsWithIgnoreCase(String prefix);

    T endsWith(String suffix);

    T endsWithIgnoreCase(String suffix);

    T contains(String string);

    T containsIgnoreCase(String suffix);

    T hasThisOne(Object val);

    T hasAllOf(Iterable<?> vals);

    T hasAnyOf(Iterable<?> vals);

    T hasNoneOf(Iterable<?> vals);

    T in(Iterable<?> vals);

    T notIn(Iterable<?> vals);

    T hasThisElement(Object val);

    T sizeEq(int val);

    T near(double x, double y);

    T near(double x, double y, boolean spherical);

    T near(double x, double y, double radius);

    T near(double x, double y, double radius, boolean spherical);

    T within(double x, double y, double radius);

    T within(double x, double y, double radius, boolean spherical);

    T within(double x1, double y1, double x2, double y2);
}
