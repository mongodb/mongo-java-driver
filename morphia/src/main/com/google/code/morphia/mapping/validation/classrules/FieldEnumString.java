/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

/**
 *
 */
package com.google.code.morphia.mapping.validation.classrules;

import com.google.code.morphia.mapping.MappedField;

import java.util.Arrays;
import java.util.List;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
public class FieldEnumString {
    private final String display;

    public FieldEnumString(final MappedField... fields) {
        this(Arrays.asList(fields));
    }

    public FieldEnumString(final List<MappedField> fields) {
        final StringBuffer sb = new StringBuffer(128);
        for (final MappedField mappedField : fields) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(mappedField.getNameToStore());
        }
        this.display = sb.toString();
    }

    @Override
    public String toString() {
        return display;
    }
}
