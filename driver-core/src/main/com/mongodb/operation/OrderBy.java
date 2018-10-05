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

package com.mongodb.operation;

/**
 * Enumeration of possible index orders
 *
 * @since 3.0
 */
@Deprecated
public enum OrderBy {
    /**
     * Ascending order
     */
    ASC(1),

    /**
     * Descending order
     */
    DESC(-1);

    private final int intRepresentation;

    OrderBy(final int intRepresentation) {
        this.intRepresentation = intRepresentation;
    }

    /**
     * The integer representation of the order.
     *
     * @return the integer representation
     */
    public int getIntRepresentation() {
        return intRepresentation;
    }

    /**
     * Gets the order from the given integer representation.
     *
     * @param intRepresentation the integer representation
     * @return the order
     */
    public static OrderBy fromInt(final int intRepresentation) {
        switch (intRepresentation) {
            case 1:
                return ASC;
            case -1:
                return DESC;
            default:
                throw new IllegalArgumentException(intRepresentation + " is not a valid index Order");
        }
    }
}
