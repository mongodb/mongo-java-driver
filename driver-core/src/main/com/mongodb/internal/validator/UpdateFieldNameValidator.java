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

package com.mongodb.internal.validator;

import org.bson.FieldNameValidator;

import static com.mongodb.assertions.Assertions.assertFalse;
import static java.lang.String.format;

/**
 * A field name validator for update documents.  It ensures that all top-level fields start with a '$'.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class UpdateFieldNameValidator implements org.bson.FieldNameValidator {
    private boolean encounteredField = false;

    @Override
    public boolean validate(final String fieldName) {
        encounteredField = true;
        return fieldName.startsWith("$");
    }

    @Override
    public String getValidationErrorMessage(final String fieldName) {
        assertFalse(fieldName.startsWith("$"));
        return format("All update operators must start with '$', but '%s' does not", fieldName);
    }

    @Override
    public FieldNameValidator getValidatorForField(final String fieldName) {
        return NoOpFieldNameValidator.INSTANCE;
    }

    @Override
    public void start() {
        encounteredField = false;
    }

    @Override
    public void end() {
        if (!encounteredField) {
            throw new IllegalArgumentException("Invalid BSON document for an update. The document may not be empty.");
        }
    }
}
