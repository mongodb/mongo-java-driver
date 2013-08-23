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

package org.mongodb.codecs;

import org.bson.BSONReader;
import org.bson.BSONWriter;
import org.mongodb.Codec;
import org.mongodb.diagnostics.Loggers;

import java.util.logging.Level;
import java.util.logging.Logger;

import static java.lang.String.format;

public class NoCodec implements Codec<Object> {
    private static final Logger LOGGER = Loggers.getLogger("codec");

    @Override
    public Object decode(final BSONReader reader) {
        LOGGER.log(Level.WARNING, "NoOpCodec used to decode an Object.  This should not be registered for decoding.");
        throw new DecodingException("NoOpCodec used to decode an Object.  This should not be registered for decoding.");
    }

    @Override
    public void encode(final BSONWriter bsonWriter, final Object value) {
        LOGGER.log(Level.WARNING, "NoOpCodec used to encode object:" + value + " of type: " + value.getClass()
                                  + ".  You should call Codecs.setDefaultObjectCodec with a custom Codec for this type"
                                  + ", or try PojoCodec.");
        throw new EncodingException(format("Could not find an encoder for object '%s' of class '%s'.",
                                           value, value.getClass()));
    }

    @Override
    public Class<Object> getEncoderClass() {
        return Object.class;
    }
}
