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

package org.bson;

import static java.lang.String.format;
import static org.bson.assertions.Assertions.isTrue;

/**
 * A field name validator, for use by BSON writers to validate field names as documents are encoded.
 *
 * @since 3.0
 */
public interface FieldNameValidator {
    /**
     * Returns true if the field name is valid, false otherwise.
     *
     * @param fieldName the field name
     * @return true if the field name is valid, false otherwise
     */
    boolean validate(String fieldName);

    /**
     * Return the validation error message for an invalid field
     *
     * @param fieldName the field name
     * @return the validation error message
     * @throws IllegalArgumentException if fieldName is actually valid
     */
    default String getValidationErrorMessage(final String fieldName) {
        isTrue(fieldName + " is valid", !validate(fieldName));
        return format("Invalid BSON field name %s", fieldName);
    }

    /**
     * Gets a new validator to use for the value of the field with the given name.
     *
     * @param fieldName the field name
     * @return a non-null validator
     */
    FieldNameValidator getValidatorForField(String fieldName);

    /**
     * Start validation of a single document.
     *
     * @since 4.0
     */
    default void start() {
    }

    /**
     * End validation of a single document.
     *
     * @since 4.0
     */
    default void end() {
    }
}
