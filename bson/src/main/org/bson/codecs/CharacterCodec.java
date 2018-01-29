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

package org.bson.codecs;

import org.bson.BsonInvalidOperationException;
import org.bson.BsonReader;
import org.bson.BsonWriter;

import static java.lang.String.format;
import static org.bson.assertions.Assertions.notNull;

/**
 * Encodes and decodes {@code Character} objects.
 *
 * @since 3.0
 */
public class CharacterCodec implements Codec<Character> {
    @Override
    public void encode(final BsonWriter writer, final Character value, final EncoderContext encoderContext) {
        notNull("value", value);

        writer.writeString(value.toString());
    }

    @Override
    public Character decode(final BsonReader reader, final DecoderContext decoderContext) {
        String string = reader.readString();
        if (string.length() != 1) {
            throw new BsonInvalidOperationException(format("Attempting to decode the string '%s' to a character, but its length is not "
                                                           + "equal to one", string));
        }

        return string.charAt(0);
    }

    @Override
    public Class<Character> getEncoderClass() {
        return Character.class;
    }
}
