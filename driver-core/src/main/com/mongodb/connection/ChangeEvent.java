/*
 * Copyright 2008-2016 MongoDB, Inc.
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

package com.mongodb.connection;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * Represents some sort of change in the system
 *
 * @param <T> the type of the value that changed.
 */
final class ChangeEvent<T> {
    private final T previousValue;
    private final T newValue;

    ChangeEvent(final T previousValue, final T newValue) {
        this.previousValue = notNull("oldValue", previousValue);
        this.newValue = notNull("newValue", newValue);
    }

    /**
     * Returns the value before this change event was fired.
     *
     * @return the previous value
     */
    public T getPreviousValue() {
        return previousValue;
    }

    /**
     * Returns the value after the event was fired
     *
     * @return the updated value
     */
    public T getNewValue() {
        return newValue;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ChangeEvent<?> that = (ChangeEvent<?>) o;

        if (!newValue.equals(that.newValue)) {
            return false;
        }

        if (previousValue != null ? !previousValue.equals(that.previousValue) : that.previousValue != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = previousValue != null ? previousValue.hashCode() : 0;
        result = 31 * result + newValue.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "ChangeEvent{"
                + "previousValue=" + previousValue
               + ", newValue=" + newValue
               + '}';
    }
}
