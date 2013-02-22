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

package org.mongodb.json;

import org.bson.BSONWriterSettings;
import org.mongodb.annotations.Immutable;

/**
 * Settings to control the behavior of a {@code JSONWriter} instance.
 *
 * @see JSONWriter
 * @since 3.0.0
 */
@Immutable
public class JSONWriterSettings extends BSONWriterSettings {
    private final boolean indent;
    private final String newLineCharacters;
    private final String indentCharacters;
    private final JSONOutputMode outputMode;

    /**
     * Builder to create an immutable instance of JSONWriterSettings
     * @since 3.0.0
     */
    public final class Builder {
        private boolean indent;
        private String newLineCharacters;
        private String indentCharacters;
        private JSONOutputMode outputMode;

        private Builder() {
        }

        public Builder indented() {
            indent = true;
            return this;
        }

    }

    /**
     * Creates a new instance with default values for all properties.
     */
    public JSONWriterSettings() {
        this(JSONOutputMode.Strict, false, null, null);
    }

    /**
     * Creates a new instance with the given output mode and default values for all other properties.
     *
     * @param outputMode the output mode
     */
    public JSONWriterSettings(final JSONOutputMode outputMode) {
        this(outputMode, false, null, null);
    }

    /**
     * Creates a new instance with indent mode enabled, and the default value for all other properties.
     *
     * @param indent whether indent mode is enabled
     */
    public JSONWriterSettings(final boolean indent) {
        this(JSONOutputMode.Strict, true, "  ", null);
    }

    /**
     * Creates a new instance with the given output mode, indent mode enabled, and the default value for all other
     * properties.
     *
     * @param outputMode the output mode
     * @param indent     whether indent mode is enabled
     */
    public JSONWriterSettings(final JSONOutputMode outputMode, final boolean indent) {
        this(outputMode, true, "  ", null);
    }

    /**
     * Creates a new instance with the given values for all properties, indent mode enabled and the default value of
     * {@code newLineCharacters}.
     *
     * @param outputMode       the output mode
     * @param indentCharacters the indent characters
     */
    public JSONWriterSettings(final JSONOutputMode outputMode, final String indentCharacters) {
        this(outputMode, true, indentCharacters, null);
    }

    /**
     * Creates a new instance with the given values for all properties and indent mode enabled.
     *
     * @param outputMode        the output mode
     * @param indentCharacters  the indent characters
     * @param newLineCharacters the new line character(s) to use
     */
    public JSONWriterSettings(final JSONOutputMode outputMode, final String indentCharacters,
                              final String newLineCharacters) {
        this(outputMode, true, indentCharacters, newLineCharacters);
    }

    private JSONWriterSettings(final JSONOutputMode outputMode, final boolean indent, final String indentCharacters,
                               final String newLineCharacters) {
        if (indent) {
            if (indentCharacters == null) {
                throw new IllegalArgumentException("indent characters can not be null if indent is enabled");
            }
        }
        else {
            if (newLineCharacters != null) {
                throw new IllegalArgumentException("new line characters can not be null if indent is disabled.");
            }
            if (indentCharacters != null) {
                throw new IllegalArgumentException("indent characters can not be null if indent is disabled.");
            }
        }
        if (outputMode == null) {
            throw new IllegalArgumentException("output mode can not be null");
        }

        this.indent = indent;
        this.newLineCharacters = newLineCharacters != null ? newLineCharacters : System.getProperty("line.separator");
        this.indentCharacters = indentCharacters;
        this.outputMode = outputMode;
    }

    /**
     * The indentation mode.  If true, output will be indented.  Otherwise, it will all be on the same line. The default
     * value is {@code false}.
     * <p/>
     * * @return whether output should be indented.
     */
    public boolean isIndent() {
        return indent;
    }

    /**
     * The new line character(s) to use if indent mode is enabled.  The default value is {@code
     * System.getProperty("line.separator")}.
     *
     * @return the new line character(s) to use.
     */
    public String getNewLineCharacters() {
        return newLineCharacters;
    }

    /**
     * The indent characters to use if indent mode is enabled.  The default value is two spaces.
     *
     * @return the indent characters to use.
     */
    public String getIndentCharacters() {
        return indentCharacters;
    }

    /**
     * The output mode to use.  The default value is {@code }JSONOutputMode.Strict}.
     *
     * @return the output mode.
     */
    public JSONOutputMode getOutputMode() {
        return outputMode;
    }
}
