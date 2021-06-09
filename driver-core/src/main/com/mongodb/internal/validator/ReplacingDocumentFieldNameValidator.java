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

import java.util.Arrays;
import java.util.List;

/**
 * A field name validator for documents that are meant for storage in MongoDB collections via replace operations. It ensures that no
 * top-level fields start with '$' (with the exception of "$db", "$ref", and "$id", so that DBRefs are not rejected).
 *
 * <p>This class should not be considered a part of the public API.</p>
 */
public class ReplacingDocumentFieldNameValidator implements FieldNameValidator {
    private static final NoOpFieldNameValidator NO_OP_FIELD_NAME_VALIDATOR = new NoOpFieldNameValidator();
    // Have to support DBRef fields
    private static final List<String> EXCEPTIONS = Arrays.asList("$db", "$ref", "$id");

    @Override
    public boolean validate(final String fieldName) {
        if (fieldName == null) {
            throw new IllegalArgumentException("Field name can not be null");
        }

        return !fieldName.startsWith("$") || EXCEPTIONS.contains(fieldName);
    }

    @Override
    public FieldNameValidator getValidatorForField(final String fieldName) {
        // Only top-level fields are validated
        return NO_OP_FIELD_NAME_VALIDATOR;
    }
}
