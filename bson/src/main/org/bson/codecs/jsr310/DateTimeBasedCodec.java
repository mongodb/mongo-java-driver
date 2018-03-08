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

package org.bson.codecs.jsr310;

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecConfigurationException;

import static java.lang.String.format;

abstract class DateTimeBasedCodec<T> implements Codec<T> {

    long validateAndReadDateTime(final BsonReader reader) {
        BsonType currentType = reader.getCurrentBsonType();
        if (!currentType.equals(BsonType.DATE_TIME)) {
            throw new CodecConfigurationException(format("Could not decode into %s, expected '%s' BsonType but got '%s'.",
                    getEncoderClass().getSimpleName(), BsonType.DATE_TIME, currentType));
        }
        return reader.readDateTime();
    }

}
