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

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CollectibleDocumentFieldNameValidatorTest {
    private final CollectibleDocumentFieldNameValidator fieldNameValidator = new CollectibleDocumentFieldNameValidator();

    @Test
    public void testFieldValidationSuccess() {
        assertTrue(fieldNameValidator.validate("ok"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullFieldNameValidation() {
        fieldNameValidator.validate(null);
    }

    @Test
    public void testFieldNameWithDotsValidation() {
        assertFalse(fieldNameValidator.validate("1.2"));
    }

    @Test
    public void testFieldNameStartsWithDollarValidation() {
        assertFalse(fieldNameValidator.validate("$1"));
        assertTrue(fieldNameValidator.validate("$db"));
        assertTrue(fieldNameValidator.validate("$ref"));
        assertTrue(fieldNameValidator.validate("$id"));
    }

}
