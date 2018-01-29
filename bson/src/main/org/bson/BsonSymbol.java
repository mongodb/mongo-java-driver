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

// Symbol.java

package org.bson;

/**
 * Class to hold a BSON symbol object, which is an interned string in Ruby
 *
 * @since 3.0
 */
public class BsonSymbol extends BsonValue {

    private final String symbol;

    /**
     * Creates a new instance.
     *
     * @param value the symbol value
     */
    public BsonSymbol(final String value) {
        if (value == null) {
            throw new IllegalArgumentException("Value can not be null");
        }
        symbol = value;
    }

    @Override
    public BsonType getBsonType() {
        return BsonType.SYMBOL;
    }

    /**
     * Gets the symbol value
     *
     * @return the symbol.
     */
    public String getSymbol() {
        return symbol;
    }

    /**
     * Will compare equal to a String that is equal to the String that this holds
     *
     * @param o the Symbol to compare this to
     * @return true if parameter o is the same as this Symbol
     */
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BsonSymbol symbol1 = (BsonSymbol) o;

        if (!symbol.equals(symbol1.symbol)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return symbol.hashCode();
    }

    @Override
    public String toString() {
        return symbol;
    }
}
