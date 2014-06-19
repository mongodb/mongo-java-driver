/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import org.bson.BsonWriterSettings;

/**
 * Settings to control the behavior of a {@code JSONWriter} instance.
 *
 * @see JsonWriter
 * @since 3.0
 */
public final class JsonWriterSettings extends BsonWriterSettings {
    private final boolean indent;
    private final String newLineCharacters;
    private final String indentCharacters;
    private final JsonMode outputMode;
    private final JsonMongoDBVersion mongoDBVersion;

    public static Builder builder() {
        return new Builder();
    }

    private JsonWriterSettings(final JsonMode outputMode, final JsonMongoDBVersion mongoDBVersion, final boolean indent,
                               final String indentCharacters, final String newLineCharacters) {

        this.outputMode = outputMode;
        this.mongoDBVersion = mongoDBVersion;
        this.indent = indent;
        this.newLineCharacters = newLineCharacters;
        this.indentCharacters = indentCharacters;

    }

    /**
     * The indentation mode.  If true, output will be indented.  Otherwise, it will all be on the same line. The default value is {@code
     * false}.
     * <p/>
     * * @return whether output should be indented.
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
     * @return the indent characters to use.
     */
    public String getIndentCharacters() {
        return indentCharacters;
    }

    /**
     * The output mode to use.  The default value is {@code }JSONMode.STRICT}.
     *
     * @return the output mode.
     */
    public JsonMode getOutputMode() {
        return outputMode;
    }

    /**
     * The MongoDB version to target.  The default value is {@code }JsonMongoDBVersion.MONGO_2_0}.
     *
     * @return JsonMongoDBVersion.
     */
    public JsonMongoDBVersion getMongoDBVersion() {
        return mongoDBVersion;
    }

    public static class Builder {
        //CHECKSTYLE:OFF
        JsonMode outputMode = JsonMode.STRICT;
        JsonMongoDBVersion mongoDBVersion = JsonMongoDBVersion.MONGO_2_0;
        boolean indent = false;
        String newLineCharacters = System.getProperty("line.separator");
        String indentCharacters = "  ";
        //CHECKSTYLE:ON

        /**
         * Sets the output mode. The default value is {@code }JSONMode.STRICT}
         *
         * @param outputMode the output mode
         * @return this
         */
        public Builder outputMode(final JsonMode outputMode) {
            this.outputMode = outputMode;
            return this;
        }

        /**
         * Sets the MongoDBVersion to target. The default value is {@code }JsonMongoDBVersion.MONGO_2_0}
         * @param mongoDBVersion The mongoDBVersion to target.
         * @return this
         */
        public Builder mongoDBVersion(final JsonMongoDBVersion mongoDBVersion) {
            this.mongoDBVersion = mongoDBVersion;
            return this;
        }

        /**
         * Set indent mode to enabled. The default value is two spaces.
         * @return this
         */
        public Builder indent() {
            this.indent = true;
            return this;
        }

        /**
         * Set the indent characters to use, automatically sets indent mode.
         * The default value with indent mode is two spaces.
         *
         * @param indentCharacters the indent characters
         * @return this
         */
        public Builder indentCharacters(final String indentCharacters) {
            this.indent = true;
            this.indentCharacters = indentCharacters;
            return this;
        }

        /**
         * Set the new line character(s) to use, automatically sets indent mode on.
         * The default value with indent mode on is {@code System.getProperty("line.separator")}.
         * @param newLineCharacters the newline characters to use
         * @return this
         */
        public Builder newLineCharacters(final String newLineCharacters) {
            this.newLineCharacters = newLineCharacters;
            this.indent = true;
            return this;
        }

        /**
         * Build the JsonWriterSettings
         * @return JsonWriterSettings
         */
        public JsonWriterSettings build() {
            return new JsonWriterSettings(outputMode, mongoDBVersion, indent, indentCharacters, newLineCharacters);
        }

        Builder() {
        }
    }

}
