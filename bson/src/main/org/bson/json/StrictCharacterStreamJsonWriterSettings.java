/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
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
     * A builder for StrictCharacterStreamJsonWriterSettings
     *
     * @since 3.4
     */
    public static final class Builder {
        private boolean indent;
        private String newLineCharacters = System.getProperty("line.separator");
        private String indentCharacters = "  ";

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

        private Builder() {
        }
    }
}
