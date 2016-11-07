/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
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

package org.bson;

import java.util.Arrays;

import static org.bson.assertions.Assertions.notNull;

/**
 * A holder class for a BSON regular expression, so that we can delay compiling into a Pattern until necessary.
 *
 * @since 3.0
 */
public final class BsonRegularExpression extends BsonValue {

    private final String pattern;
    private final String options;

    /**
     * Creates a new instance
     *
     * @param pattern the regular expression {@link java.util.regex.Pattern}
     * @param options the options for the regular expression
     */
    public BsonRegularExpression(final String pattern, final String options) {
        this.pattern = notNull("pattern", pattern);
        this.options = options == null ? "" : sortOptionCharacters(options);
    }

    /**
     * Creates a new instance with no options set.
     *
     * @param pattern the regular expression {@link java.util.regex.Pattern}
     */
    public BsonRegularExpression(final String pattern) {
        this(pattern, null);
    }

    @Override
    public BsonType getBsonType() {
        return BsonType.REGULAR_EXPRESSION;
    }

    /**
     * Gets the regex pattern.
     *
     * @return the regular expression pattern
     */
    public String getPattern() {
        return pattern;
    }

    /**
     * Gets the options for the regular expression
     *
     * @return the options.
     */
    public String getOptions() {
        return options;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BsonRegularExpression that = (BsonRegularExpression) o;

        if (!options.equals(that.options)) {
            return false;
        }
        if (!pattern.equals(that.pattern)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = pattern.hashCode();
        result = 31 * result + options.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "BsonRegularExpression{"
               + "pattern='" + pattern + '\''
               + ", options='" + options + '\''
               + '}';
    }

    private String sortOptionCharacters(final String options) {
        char[] chars = options.toCharArray();
        Arrays.sort(chars);
        return new String(chars);
    }
}
