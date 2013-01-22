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

package com.google.code.morphia.utils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;

@SuppressWarnings({ "rawtypes", "unchecked" })
public final class Assert {
    private Assert() {
        // hide
    }

    public static void isNotNull(final Object reference) {
        Assert.isNotNull(reference, "Reference was null");
    }

    public static void isNotNull(final Object reference, final String msg) {
        if (reference == null) {
            Assert.raiseError("Reference was null: " + msg);
        }
    }

    public static void isNotNullAndNotEmpty(final String reference) {
        Assert.isNotNullAndNotEmpty(reference, "Reference was null or empty");
    }

    public static void isNotNullAndNotEmpty(final String reference, final String msg) {
        if ((reference == null) || (reference.length() == 0)) {
            Assert.raiseError(msg);
        }
    }

    public static void isFalse(final boolean value) {
        Assert.isFalse(value, "Value was true");
    }

    public static void isFalse(final boolean value, final String msg) {
        if (value) {
            Assert.raiseError(msg);
        }
    }

    public static void isNotFalse(final boolean value) {
        Assert.isNotFalse(value, "Value was false");
    }

    public static void isNotFalse(final boolean value, final String msg) {
        if (!value) {
            Assert.raiseError(msg);
        }
    }

    public static void isTrue(final boolean value) {
        Assert.isTrue(value, "Value was false");
    }

    public static void isTrue(final boolean value, final String msg) {
        if (!value) {
            Assert.raiseError(msg);
        }
    }

    public static void isNotTrue(final boolean value) {
        Assert.isNotTrue(value, "Value was true");
    }

    public static void isNotTrue(final boolean value, final String msg) {
        if (value) {
            Assert.raiseError(msg);
        }
    }

    public static void raiseError(final String error) {
        throw new AssertionFailedException(error);
    }

    public static void raiseError(final String error, final Exception e) {
        throw new AssertionFailedException(error, e);
    }

    public static class AssertionFailedException extends RuntimeException {

        private static final long serialVersionUID = 435272532743543854L;

        public AssertionFailedException() {
        }

        public AssertionFailedException(final String detail) {
            super(detail);
        }

        public AssertionFailedException(final String detail, final Throwable e) {
            super(detail, e);
        }
    }

    public static void isEqual(final Object obj1, final Object obj2) {
        if ((obj1 == null) && (obj2 == null)) {
            return;
        }
        if ((obj1 != null) && (!obj1.equals(obj2))) {
            Assert.raiseError("'" + obj1 + "' and '" + obj2 + "' are not equal");
        }
    }

    public static void isNotEmpty(final Collection<?> collection) {
        if (collection == null) {
            Assert.raiseError("'collection' is null");
        }
        else if (collection.isEmpty()) {
            Assert.raiseError("'collection' is empty");
        }
    }

    public static void isEmpty(final Collection<?> collection) {
        if (collection == null) {
            Assert.raiseError("'collection' is null");
        }
        else if (!collection.isEmpty()) {
            Assert.raiseError("'collection' is not empty");
        }
    }

    public static void parametersNotNull(final String nameOfParameters, Object... objects) {
        String msgPrefix = "At least one of the parameters ";
        final String msgSuffix = " is null.";

        if (objects == null) {
            objects = new Object[]{null};
        }

        if (objects.length == 1) {
            msgPrefix = "Parameter ";
        }

        for (final Object object : objects) {
            if (object == null) {
                Assert.raiseError(msgPrefix + "'" + nameOfParameters + "' " + msgSuffix);
            }
        }
    }

    public static void parameterNotNull(final Object reference, final String nameOfParameter) {
        if (reference == null) {
            Assert.raiseError("Parameter '" + nameOfParameter + "' is not expected to be null.");
        }
    }

    public static void isNull(final Object object, final String string) {
        if (object != null) {
            Assert.raiseError(string);
        }
    }

    public static void parameterInRange(final int value, final int min, final int max, final String string) {
        Assert.isTrue((min <= value) && (value <= max), "Parameter '" + string + "' must be in range of " + min
                                                        + " <= " + string + " <= " + max + ". Current value was "
                                                        + value);

    }

    public static void parameterLegal(final boolean condition, final String parameter) {
        Assert.isTrue(condition, "Parameter '" + parameter + "' is not legal.");
    }

    public static void parameterInRange(final long value, final long min, final long max, final String name) {
        Assert.isTrue((min <= value) && (value <= max), "Parameter '" + name + "' must be in range of " + min + " <= "
                                                        + name + " <= " + max + ". Current value was " + value);
    }

    public static void implementsSerializable(final Object toTest) {
        if (!Serializable.class.isAssignableFrom(toTest.getClass())) {
            Assert.raiseError("Object of class '" + toTest.getClass() + "' does not implement Serializable");
        }
    }

    public static void parameterInstanceOf(final Object obj, final Class<?> class1, final String paramName) {
        if (!class1.isAssignableFrom(obj.getClass())) {
            Assert.raiseError("Parameter '" + paramName + "' from type '" + obj.getClass().getName()
                              + "' was expected to implement '" + class1 + "'");
        }
    }

    public static <T extends Enum<T>> void enumContains(final EnumSet<T> enumSetToLookIn, final T enumToLookFor) {
        Assert.isTrue(enumSetToLookIn.contains(enumToLookFor), "Mode " + enumToLookFor
                                                               + " is not part of the enum given : " + enumSetToLookIn);
    }

    public static void parameterNotEmpty(final Collection obj, final String paramName) {
        if (obj.isEmpty()) {
            Assert.raiseError("Parameter '" + paramName + "' from type '" + obj.getClass().getName()
                              + "' is expected to NOT be empty.");
        }
    }

    public static void parameterNotEmpty(final Iterable obj, final String paramName) {
        if (!obj.iterator().hasNext()) {
            Assert.raiseError("Parameter '" + paramName + "' from type '" + obj.getClass().getName()
                              + "' is expected to NOT be empty");
        }
    }

    public static void parameterNotEmpty(final String reference, final String nameOfParameter) {

        if ("".equals(reference)) {
            Assert.raiseError("Parameter '" + nameOfParameter + "' is expected to NOT be empty.");
        }
    }

    public static <T> void parameterInCollection(final T object, final Collection<? super T> collection) {
        if (collection == null) {
            Assert.raiseError("'collection' is null");
        }
        if (!collection.contains(object)) {
            Assert.raiseError("Parameter '" + object + "' not in collection '" + collection + "'");
        }
    }

    public static void isNotSame(final Object a, final Object b) {
        if (a == b) {
            raiseError("References are expected to be different");
        }
    }

    public static void isNotEmpty(final Object[] array) {
        if (array == null) {
            Assert.raiseError("'array' is null");
        }
        else if (array.length == 0) {
            Assert.raiseError("'array' is empty");
        }

    }

    public static void isEmpty(final Object[] array) {
        if (array == null) {
            Assert.raiseError("'array' is null");
        }
        else if (array.length != 0) {
            Assert.raiseError("'array' is not empty");
        }

    }

    public static void parameterArraysOfSameLength(final String argNames, final Object[]... objects) {
        for (final Object[] object : objects) {
            Assert.parametersNotNull("objects", object);
        }

        final int length = objects.length;

        parameterLegal(length >= 2, "Parameter objects was expected to contains at least two arrays.");

        int size = -99;
        for (int i = 0; i < length; i++) {
            final Object[] o = objects[i];

            int sizeOfO = -1;
            if (o != null) {
                sizeOfO = o.length;
            }

            if ((i > 0) && (sizeOfO != size)) {
                raiseError("At least one of '" + argNames + "' differs in length");
            }

            size = sizeOfO;
        }
    }

    public static void parameterIterableDoesNotContainNull(final String string, final Iterable<?> collection) {
        Assert.parametersNotNull("string, collection", string, collection);
        for (final Object object : collection) {
            if (object == null) {
                raiseError("Iterable Parameter '" + string + "' must not contain null.");
            }
        }
    }

    public static void parameterIterableDoesNotContainNull(final String string, final Object[] a) {
        parameterIterableDoesNotContainNull(string, Arrays.asList(a));
    }

    public static void isSame(final Object a, final Object b) {
        Assert.isTrue(a == b, "Expected to be the same (not only equal, but same reference)");
    }

}
