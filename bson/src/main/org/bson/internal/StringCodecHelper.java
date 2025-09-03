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

package org.bson.internal;

import org.bson.BsonInvalidOperationException;
import org.bson.BsonReader;
import org.bson.BsonType;

import static java.lang.String.format;

/**
 * This class is not part of the public API. It may be removed or changed at any time.
 */
public final class StringCodecHelper {

    private StringCodecHelper(){
        //NOP
    }

    public static char decodeChar(final BsonReader reader) {
        BsonType currentBsonType = reader.getCurrentBsonType();
        if (currentBsonType != BsonType.STRING) {
            throw new BsonInvalidOperationException(format("Invalid string type, found: %s", currentBsonType));
        }
        String string = reader.readString();
        if (string.length() != 1) {
            throw new BsonInvalidOperationException(format("Attempting to decode the string '%s' to a character, but its length is not "
                    + "equal to one", string));
        }
        return string.charAt(0);
    }
}
