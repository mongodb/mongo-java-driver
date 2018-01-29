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

import com.mongodb.annotations.NotThreadSafe;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;

/**
 * The options regarding collation support in MongoDB 3.4+
 *
 * @mongodb.driver.manual reference/method/db.createCollection/ Create Collection
 * @mongodb.driver.manual reference/command/createIndexes Index options
 * @since 3.4
 * @mongodb.server.release 3.4
 */
public final class Collation {
    private final String locale;
    private final Boolean caseLevel;
    private final CollationCaseFirst caseFirst;
    private final CollationStrength strength;
    private final Boolean numericOrdering;
    private final CollationAlternate alternate;
    private final CollationMaxVariable maxVariable;
    private final Boolean normalization;
    private final Boolean backwards;

    /**
     * Convenience method to create a Builder.
     *
     * @return a builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Convenience method to create a from an existing {@code Collation}.
     *
     * @param options create a builder from existing options
     * @return a builder
     */
    public static Builder builder(final Collation options) {
        return new Builder(options);
    }

    /**
     * A Collation builder.
     */
    @NotThreadSafe
    public static final class Builder {
        private String locale;
        private Boolean caseLevel;
        private CollationCaseFirst caseFirst;
        private CollationStrength strength;
        private Boolean numericOrdering;
        private CollationAlternate alternate;
        private CollationMaxVariable maxVariable;
        private Boolean normalization;
        private Boolean backwards;

        private Builder() {
        }

        private Builder(final Collation options) {
            this.locale = options.getLocale();
            this.caseLevel = options.getCaseLevel();
            this.caseFirst = options.getCaseFirst();
            this.strength = options.getStrength();
            this.numericOrdering = options.getNumericOrdering();
            this.alternate = options.getAlternate();
            this.maxVariable = options.getMaxVariable();
            this.normalization = options.getNormalization();
            this.backwards = options.getBackwards();
        }

        /**
         * Sets the locale
         *
         * @param locale the locale
         * @see <a href="http://userguide.icu-project.org/locale">ICU User Guide - Locale</a>
         * @return this
         */
        public Builder locale(final String locale) {
            this.locale = locale;
            return this;
        }

        /**
         * Sets the case level value
         *
         * <p>Turns on case sensitivity</p>
         * @param caseLevel the case level value
         * @return this
         */
        public Builder caseLevel(final Boolean caseLevel) {
            this.caseLevel = caseLevel;
            return this;
        }

        /**
         * Sets the collation case first value
         *
         * <p>Determines if Uppercase or lowercase values should come first</p>
         * @param caseFirst the collation case first value
         * @return this
         */
        public Builder collationCaseFirst(final CollationCaseFirst caseFirst) {
            this.caseFirst = caseFirst;
            return this;
        }

        /**
         * Sets the collation strength
         *
         * @param strength the strength
         * @return this
         */
        public Builder collationStrength(final CollationStrength strength) {
            this.strength = strength;
            return this;
        }

        /**
         * Sets the numeric ordering
         *
         * @param numericOrdering if true will order numbers based on numerical order and not collation order
         * @return this
         */
        public Builder numericOrdering(final Boolean numericOrdering) {
            this.numericOrdering = numericOrdering;
            return this;
        }

        /**
         * Sets the alternate
         *
         * <p>Controls whether spaces and punctuation are considered base characters</p>
         *
         * @param alternate the alternate
         * @return this
         */
        public Builder collationAlternate(final CollationAlternate alternate) {
            this.alternate = alternate;
            return this;
        }

        /**
         * Sets the maxVariable
         *
         * @param maxVariable the maxVariable
         * @return this
         */
        public Builder collationMaxVariable(final CollationMaxVariable maxVariable) {
            this.maxVariable = maxVariable;
            return this;
        }

        /**
         * Sets the normalization value
         *
         * <p>If true, normalizes text into Unicode NFD.</p>
         * @param normalization the normalization value
         * @return this
         */
        public Builder normalization(final Boolean normalization) {
            this.normalization = normalization;
            return this;
        }

        /**
         * Sets the backwards value
         *
         * <p>Causes secondary differences to be considered in reverse order, as it is done in the French language</p>
         *
         * @param backwards the backwards value
         * @return this
         */
        public Builder backwards(final Boolean backwards) {
            this.backwards = backwards;
            return this;
        }

        /**
         * Creates a new Collation object with the settings initialised on this builder.
         *
         * @return a new Collation object
         */
        public Collation build() {
            return new Collation(this);
        }
    }

    /**
     * Returns the locale
     *
     * @see <a href="http://userguide.icu-project.org/locale">ICU User Guide - Locale</a>
     * @return the locale
     */
    public String getLocale() {
        return locale;
    }

    /**
     * Returns the case level value
     *
     * @return the case level value
     */
    public Boolean getCaseLevel() {
        return caseLevel;
    }

    /**
     * Returns the collation case first value
     *
     * @return the collation case first value
     */
    public CollationCaseFirst getCaseFirst() {
        return caseFirst;
    }

    /**
     * Returns the collation strength
     *
     * @return the collation strength
     */
    public CollationStrength getStrength() {
        return strength;
    }

    /**
     * Returns the numeric ordering, if true will order numbers based on numerical order and not collation order.
     *
     * @return the numeric ordering
     */
    public Boolean getNumericOrdering() {
        return numericOrdering;
    }

    /**
     * Returns the collation alternate
     *
     * @return the alternate
     */
    public CollationAlternate getAlternate() {
        return alternate;
    }

    /**
     * Returns the maxVariable
     *
     * <p>Controls which characters are affected by collection alternate {@link CollationAlternate#SHIFTED}.</p>
     * @return the maxVariable
     */
    public CollationMaxVariable getMaxVariable() {
        return maxVariable;
    }

    /**
     * Returns the normalization value
     *
     * <p>If true, normalizes text into Unicode NFD.</p>
     * @return the normalization
     */
    public Boolean getNormalization() {
        return normalization;
    }

    /**
     * Returns the backwards value
     *
     * @return the backwards value
     */
    public Boolean getBackwards() {
        return backwards;
    }

    /**
     * Gets this collation options as a document.
     *
     * @return The collation options as a BsonDocument
     */
    public BsonDocument asDocument() {
        BsonDocument collation = new BsonDocument();
        if (locale != null) {
            collation.put("locale", new BsonString(locale));
        }
        if (caseLevel != null) {
            collation.put("caseLevel", new BsonBoolean(caseLevel));
        }
        if (caseFirst != null) {
            collation.put("caseFirst", new BsonString(caseFirst.getValue()));
        }
        if (strength != null) {
            collation.put("strength", new BsonInt32(strength.getIntRepresentation()));
        }
        if (numericOrdering != null) {
            collation.put("numericOrdering", new BsonBoolean(numericOrdering));
        }
        if (alternate != null) {
            collation.put("alternate", new BsonString(alternate.getValue()));
        }
        if (maxVariable != null) {
            collation.put("maxVariable", new BsonString(maxVariable.getValue()));
        }
        if (normalization != null) {
            collation.put("normalization", new BsonBoolean(normalization));
        }
        if (backwards != null) {
            collation.put("backwards", new BsonBoolean(backwards));
        }
        return collation;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o){
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Collation that = (Collation) o;

        if (getLocale() != null ? !getLocale().equals(that.getLocale()) : that.getLocale() != null) {
            return false;
        }
        if (getCaseLevel() != null ? !getCaseLevel().equals(that.getCaseLevel()) : that.getCaseLevel() != null) {
            return false;
        }
        if (getCaseFirst() != that.getCaseFirst()) {
            return false;
        }
        if (getStrength() != that.getStrength()) {
            return false;
        }
        if (getNumericOrdering() != null ? !getNumericOrdering().equals(that.getNumericOrdering()) : that.getNumericOrdering() != null) {
            return false;
        }
        if (getAlternate() != that.getAlternate()) {
            return false;
        }
        if (getMaxVariable() != that.getMaxVariable()) {
            return false;
        }
        if (getNormalization() != null ? !getNormalization().equals(that.getNormalization()) : that.getNormalization() != null) {
            return false;
        }
        if (getBackwards() != null ? !getBackwards().equals(that.getBackwards()) : that.getBackwards() != null) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = getLocale() != null ? getLocale().hashCode() : 0;
        result = 31 * result + (getCaseLevel() != null ? getCaseLevel().hashCode() : 0);
        result = 31 * result + (getCaseFirst() != null ? getCaseFirst().hashCode() : 0);
        result = 31 * result + (getStrength() != null ? getStrength().hashCode() : 0);
        result = 31 * result + (getNumericOrdering() != null ? getNumericOrdering().hashCode() : 0);
        result = 31 * result + (getAlternate() != null ? getAlternate().hashCode() : 0);
        result = 31 * result + (getMaxVariable() != null ? getMaxVariable().hashCode() : 0);
        result = 31 * result + (getNormalization() != null ? getNormalization().hashCode() : 0);
        result = 31 * result + (getBackwards() != null ? getBackwards().hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Collation{"
                + "locale='" + locale + "'"
                + ", caseLevel=" + caseLevel
                + ", caseFirst=" + caseFirst
                + ", strength=" + strength
                + ", numericOrdering=" + numericOrdering
                + ", alternate=" + alternate
                + ", maxVariable=" + maxVariable
                + ", normalization=" + normalization
                + ", backwards=" + backwards
                + "}";
    }


    private Collation(final Builder builder) {
        this.locale = builder.locale;
        this.caseLevel = builder.caseLevel;
        this.caseFirst = builder.caseFirst;
        this.strength = builder.strength;
        this.numericOrdering = builder.numericOrdering;
        this.alternate = builder.alternate;
        this.maxVariable = builder.maxVariable;
        this.normalization = builder.normalization;
        this.backwards = builder.backwards;
    }
}
