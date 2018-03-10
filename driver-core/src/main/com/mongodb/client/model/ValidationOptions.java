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

import com.mongodb.lang.Nullable;
import org.bson.conversions.Bson;

/**
 * Validation options for documents being inserted or updated in a collection
 *
 * @since 3.2
 * @mongodb.server.release 3.2
 * @mongodb.driver.manual reference/method/db.createCollection/ Create Collection
 */
public final class ValidationOptions {
    private Bson validator;
    private ValidationLevel validationLevel;
    private ValidationAction validationAction;

    /**
     * Gets the validation rules if set or null.
     *
     * @return the validation rules if set or null
     */
    @Nullable
    public Bson getValidator() {
        return validator;
    }

    /**
     * Sets the validation rules for all
     *
     * @param validator the validation rules
     * @return this
     */
    public ValidationOptions validator(@Nullable final Bson validator) {
        this.validator = validator;
        return this;
    }

    /**
     * Gets the {@link ValidationLevel} that determines how strictly MongoDB applies the validation rules to existing documents during an
     * insert or update.
     *
     * @return the ValidationLevel.
     */
    @Nullable
    public ValidationLevel getValidationLevel() {
        return validationLevel;
    }

    /**
     * Sets the validation level that determines how strictly MongoDB applies the validation rules to existing documents during an insert
     * or update.
     *
     * @param validationLevel the validation level
     * @return this
     */
    public ValidationOptions validationLevel(@Nullable final ValidationLevel validationLevel) {
        this.validationLevel = validationLevel;
        return this;
    }

    /**
     * Gets the {@link ValidationAction}.
     *
     * @return the ValidationAction.
     */
    @Nullable
    public ValidationAction getValidationAction() {
        return validationAction;
    }

    /**
     * Sets the {@link ValidationAction} that determines whether to error on invalid documents or just warn about the violations but allow
     * invalid documents.
     *
     * @param validationAction the validation action
     * @return this
     */
    public ValidationOptions validationAction(@Nullable final ValidationAction validationAction) {
        this.validationAction = validationAction;
        return this;
    }

    @Override
    public String toString() {
        return "ValidationOptions{"
                + "validator=" + validator
                + ", validationLevel=" + validationLevel
                + ", validationAction=" + validationAction
                + '}';
    }
}
