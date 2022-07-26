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

import java.util.Map;

/**
 * A field name validator that serves as a root validator for a map of validators that are applied to child fields.  Note that instances of
 * this class can be nested to achieve a wide variety of validation behaviors.
 *
 * <p>This class should not be considered a part of the public API.</p>
 */
public class MappedFieldNameValidator implements FieldNameValidator {
    private final FieldNameValidator defaultValidator;
    private final Map<String, FieldNameValidator> fieldNameToValidatorMap;

    /**
     * The default validator will be use to validate all fields whose names are not contained int the fieldNameToValidator map.  The map is
     * used to apply different validators to fields with specific names.
     *
     * @param defaultValidator        the validator to use for any fields not matching any field name in the map
     * @param fieldNameToValidatorMap a map from field name to FieldNameValidator
     */
    public MappedFieldNameValidator(final FieldNameValidator defaultValidator,
                                    final Map<String, FieldNameValidator> fieldNameToValidatorMap) {
        this.defaultValidator = defaultValidator;
        this.fieldNameToValidatorMap = fieldNameToValidatorMap;
    }

    @Override
    public boolean validate(final String fieldName) {
        return defaultValidator.validate(fieldName);
    }

    @Override
    public String getValidationErrorMessage(final String fieldName) {
        return defaultValidator.getValidationErrorMessage(fieldName);
    }

    @Override
    public FieldNameValidator getValidatorForField(final String fieldName) {
        if (fieldNameToValidatorMap.containsKey(fieldName)) {
            return fieldNameToValidatorMap.get(fieldName);
        } else {
            return defaultValidator;
        }
    }
}
