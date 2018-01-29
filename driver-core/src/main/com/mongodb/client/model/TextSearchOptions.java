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

package com.mongodb.client.model;

/**
 * Text search options for the {@link Filters#text(String, TextSearchOptions)} helper
 *
 * @mongodb.driver.manual reference/operator/query/text $text
 * @since 3.2
 */
public final class TextSearchOptions {

    private String language;
    private Boolean caseSensitive;
    private Boolean diacriticSensitive;

    /**
     * Returns the language to be used with the text search
     *
     * @return the language to use for the text search if set or null
     */
    public String getLanguage() {
        return language;
    }

    /**
     * Set the language for the text search
     *
     * @param language the language to use for the text search
     * @return this
     */
    public TextSearchOptions language(final String language) {
        this.language = language;
        return this;
    }

    /**
     * Returns the case-sensitive flag to use with the text search
     *
     * @return the case-sensitive flag if set or null
     * @mongodb.server.release 3.2
     */
    public Boolean getCaseSensitive() {
        return caseSensitive;
    }

    /**
     * Set the case-sensitive flag for the text search
     *
     * @param caseSensitive the case-sensitive flag for the text search
     * @return this
     * @mongodb.server.release 3.2
     */
    public TextSearchOptions caseSensitive(final Boolean caseSensitive) {
        this.caseSensitive = caseSensitive;
        return this;
    }

    /**
     * Returns the diacritic-sensitive flag to use with the text search
     *
     * @return the diacritic-sensitive flag if set or null
     * @mongodb.server.release 3.2
     */
    public Boolean getDiacriticSensitive() {
        return diacriticSensitive;
    }

    /**
     * Set the diacritic-sensitive flag for the text search
     *
     * @param diacriticSensitive the diacritic-sensitive flag for the text search
     * @return this
     * @mongodb.server.release 3.2
     */
    public TextSearchOptions diacriticSensitive(final Boolean diacriticSensitive) {
        this.diacriticSensitive = diacriticSensitive;
        return this;
    }

    @Override
    public String toString() {
        return "Text Search Options{"
                       + "language='" + language + '\''
                       + ", caseSensitive=" + caseSensitive
                       + ", diacriticSensitive=" + diacriticSensitive
                       + '}';
    }
}
