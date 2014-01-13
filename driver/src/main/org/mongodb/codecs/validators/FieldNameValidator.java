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

import static java.lang.String.format;

public class FieldNameValidator implements Validator<String> {
    @Override
    public void validate(final String value) {
        if (value == null) {
            throw new IllegalArgumentException("Key can not be null");
        }

        if (value.contains(".")) {
            throw new IllegalArgumentException(format("Fields stored in the db can't have . in them. "
                                                      + "(Bad Key: '%s')", value));
        }
        if (value.startsWith("$")) {
            throw new IllegalArgumentException(format("Fields stored in the db can't start with '$' "
                                                      + "(Bad Key: '%s')", value));
        }
    }
}
