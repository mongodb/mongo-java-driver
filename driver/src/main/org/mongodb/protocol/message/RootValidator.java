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

package org.mongodb.protocol.message;

import org.bson.FieldNameValidator;

import java.util.Map;

class RootValidator implements FieldNameValidator {
    private final FieldNameValidator defaultValidator;
    private final Map<String, FieldNameValidator> pathToValidatorMap;

    RootValidator(final FieldNameValidator defaultValidator, final Map<String, FieldNameValidator> pathToValidatorMap) {
        this.defaultValidator = defaultValidator;
        this.pathToValidatorMap = pathToValidatorMap;
    }

    @Override
    public boolean validate(final String fieldName) {
        return defaultValidator.validate(fieldName);
    }

    @Override
    public FieldNameValidator getValidatorForField(final String fieldName) {
        if (pathToValidatorMap.containsKey(fieldName)) {
            return pathToValidatorMap.get(fieldName);
        } else {
            return defaultValidator;
        }
    }
}
