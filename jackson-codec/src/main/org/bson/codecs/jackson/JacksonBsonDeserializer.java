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

package org.bson.codecs.jackson;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;

/**
 * Created by guo on 8/1/14.
 */
abstract class JacksonBsonDeserializer<T> extends JsonDeserializer<T> {
    @Override
    public T deserialize(final JsonParser jsonParser, final DeserializationContext ctxt)
            throws IOException {
        if (!(jsonParser instanceof JacksonBsonParser)) {
            throw new JsonGenerationException("BsonDeserializer can "
                    + "only be used with JacksonBsonParser");
        }
        return deserialize((JacksonBsonParser) jsonParser, ctxt);
    }

    public abstract T deserialize(JacksonBsonParser bp, DeserializationContext ctxt)
            throws IOException;
}
