/*
 * Copyright (c) 2008 MongoDB, Inc.
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

package org.mongodb.codecs.validators;

import org.junit.Test;

public class FieldNameValidatorTest {
    private final FieldNameValidator fieldNameValidator = new FieldNameValidator();

    @Test
    public void testFieldValidationSuccess() {
        fieldNameValidator.validate("ok");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullFieldNameValidation() {
        fieldNameValidator.validate(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFieldNameWithDotsValidation() {
        fieldNameValidator.validate("1.2");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFieldNameStartsWithDollarValidation() {
        fieldNameValidator.validate("$1");
    }
}
