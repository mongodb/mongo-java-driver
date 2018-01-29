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

package org.bson.codecs.pojo.entities;


import java.util.NoSuchElementException;

public abstract class Optional<T> {

    private static final Optional<Object> NONE = new Optional<Object>() {
        @Override
        public Object get() {
            throw new NoSuchElementException(".get call on None!");
        }

        @Override
        public boolean isEmpty() {
            return true;
        }
    };

    @SuppressWarnings("unchecked")
    public static <T> Optional<T> empty() {
        return (Optional<T>) NONE;
    }

    @SuppressWarnings("unchecked")
    public static <T> Optional<T> of(final T it) {
        if (it == null) {
            return (Optional<T>) Optional.NONE;
        } else {
            return new Optional.Some<T>(it);
        }
    }

    public abstract T get();

    public abstract boolean isEmpty();

    @Override
    public String toString() {
        return "None";
    }

    public boolean isDefined() {
        return !isEmpty();
    }

    public static class Some<T> extends Optional<T> {
        private final T value;

        Some(final T value) {
            this.value = value;
        }

        @Override
        public T get() {
            return value;
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public String toString() {
            return String.format("Some(%s)", value);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Some<?> some = (Some<?>) o;
            return value.equals(some.value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }
    }
}
