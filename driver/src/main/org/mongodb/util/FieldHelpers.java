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

package org.mongodb.util;

import org.bson.types.BsonBoolean;
import org.bson.types.BsonNull;
import org.bson.types.BsonNumber;
import org.bson.types.BsonValue;

// TODO: Not sure about this class.  Is it generally applicable enough to be public?
public final class FieldHelpers {

    public static boolean asBoolean(final BsonValue fieldValue) {
        if (fieldValue instanceof BsonNull) {
            return false;
        } else if (fieldValue instanceof BsonBoolean) {
            return fieldValue.asBoolean().getValue();
        } else if (fieldValue instanceof BsonNumber) {
            return fieldValue.asNumber().doubleValue() != 0;
        } else {
            throw new IllegalArgumentException("value is of type " + fieldValue.getClass()
                                               + " and can not be converted to a boolean.");
        }
    }

    private FieldHelpers() {
    }
}
