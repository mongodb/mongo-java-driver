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

package org.bson.json;

import static org.bson.assertions.Assertions.notNull;

/**
 * Settings to control the behavior of a {@code JSONWriter} instance.
 *
 * @see StrictCharacterStreamJsonWriter
 * @since 3.5
 */
public final class StrictCharacterStreamJsonWriterSettings {

    private final boolean indent;
    private final String newLineCharacters;
    private final String indentCharacters;
    private final int maxLength;
    private final boolean oneArrayElementPerLine;
    private final PropertySeparator propertySeparator;

    /**
     * Create a builder for StrictCharacterStreamJsonWriterSettings, which are immutable.
     *
     * @return a Builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    private StrictCharacterStreamJsonWriterSettings(final Builder builder) {
        indent = builder.indent;
        newLineCharacters = builder.newLineCharacters != null ? builder.newLineCharacters : System.getProperty("line.separator");
        indentCharacters = builder.indentCharacters;
        maxLength = builder.maxLength;
        oneArrayElementPerLine = builder.oneArrayElementPerLine;
        propertySeparator = builder.propertySeparator;
    }

    /**
     * The indentation mode.  If true, output will be indented.  Otherwise, it will all be on the same line. The default value is {@code
     * false}.
     *
     * @return whether output should be indented.
     */
    public boolean isIndent() {
        return indent;
    }

    /**
     * The new line character(s) to use if indent mode is enabled.  The default value is {@code System.getProperty("line.separator")}.
     *
     * @return the new line character(s) to use.
     */
    public String getNewLineCharacters() {
        return newLineCharacters;
    }

    /**
     * The indent characters to use if indent mode is enabled.  The default value is two spaces.
     *
     * @return the indent character(s) to use.
     */
    public String getIndentCharacters() {
        return indentCharacters;
    }

    /**
     * The maximum length of the JSON string.  The string will be truncated at this length.
     *
     * @return the maximum length of the JSON string
     * @since 3.7
     */
    public int getMaxLength() {
        return maxLength;
    }

    /**
     * If each array element should be formatted to have its own line when indent mode is enabled.  The default value is {@code false}.
     *
     * @return if each array element should have its own line
     * @since 3.10
     */
    public boolean isOneArrayElementPerLine() {
        return oneArrayElementPerLine;
    }

    /**
     * The format of the separator between property name's and property value's.
     *
     * @return the property separator to use.
     * @since 3.10
     */
    public PropertySeparator getPropertySeparator() {
        return propertySeparator;
    }

    /**
     * The format of the separator between property name's and property value's.
     *
     * @since 3.10
     */
    public enum PropertySeparator {
        NO_SPACES(":"),
        SPACE_BEFORE(" :"),
        SPACE_AFTER(": "),
        SPACE_BEFORE_AND_AFTER(" : ");

        private String separator;

        PropertySeparator(final String separator) {
            this.separator = separator;
        }

        /**
         * The character(s) to use as a separator between property name's and property value's.
         * @return the separator character(s)
         */
        public String getSeparator() {
            return separator;
        }
    }

    /**
     * A builder for StrictCharacterStreamJsonWriterSettings
     *
     * @since 3.4
     */
    public static final class Builder {
        private boolean indent;
        private String newLineCharacters = System.getProperty("line.separator");
        private String indentCharacters = "  ";
        private int maxLength;
        private boolean oneArrayElementPerLine;
        private PropertySeparator propertySeparator = PropertySeparator.SPACE_BEFORE_AND_AFTER;

        /**
         * Build a JsonWriterSettings instance.
         *
         * @return a JsonWriterSettings instance
         */
        public StrictCharacterStreamJsonWriterSettings build() {
            return new StrictCharacterStreamJsonWriterSettings(this);
        }

        /**
         * Sets whether indentation is enabled.
         *
         * @param indent whether indentation is enabled
         * @return this
         */
        public Builder indent(final boolean indent) {
            this.indent = indent;
            return this;
        }

        /**
         * Sets the new line character string to use when indentation is enabled.
         *
         * @param newLineCharacters the non-null new line character string
         * @return this
         */
        public Builder newLineCharacters(final String newLineCharacters) {
            notNull("newLineCharacters", newLineCharacters);
            this.newLineCharacters = newLineCharacters;
            return this;
        }

        /**
         * Sets the indent character string to use when indentation is enabled.
         *
         * @param indentCharacters the non-null indent character string
         * @return this
         */
        public Builder indentCharacters(final String indentCharacters) {
            notNull("indentCharacters", indentCharacters);
            this.indentCharacters = indentCharacters;
            return this;
        }

        /**
         * Sets the maximum length of the JSON string.  The string will be truncated at this length.
         *
         * @param maxLength the maximum length, which must be &gt;= 0 where 0 indicate no maximum length
         * @return the maximum length of the JSON string
         * @since 3.7
         */
        public Builder maxLength(final int maxLength) {
            this.maxLength = maxLength;
            return this;
        }

        /**
         * Sets if each array element should be formatted to have its own line if indent mode is enabled.
         *
         * @param oneArrayElementPerLine if each array element should be formatted to have its own line
         * @return this
         * @since 3.10
         */
        public Builder oneArrayElementPerLine(final boolean oneArrayElementPerLine) {
            this.oneArrayElementPerLine = oneArrayElementPerLine;
            return this;
        }

        /**
         * Sets the format to use for the separator between property name's and property value's.
         *
         * @param propertySeparator the property separator type
         * @return this
         * @since 3.10
         */
        public Builder propertySeparator(final PropertySeparator propertySeparator) {
            notNull("propertySeparator", propertySeparator);
            this.propertySeparator = propertySeparator;
            return this;
        }

        private Builder() {
        }
    }
}
