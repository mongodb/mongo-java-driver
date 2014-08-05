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
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

/**
 * Created by guo on 7/31/14.
 */
abstract class JacksonBsonSerializer<T> extends JsonSerializer<T> {
    @Override
    public void serialize(T t, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
        if (!(jsonGenerator instanceof JacksonBsonGenerator)) {
            throw new JsonGenerationException("JacksonBsonSerializer can " +
                    "only be used with JacksonBsonGenerator");
        }
        serialize(t, (JacksonBsonGenerator<T>) jsonGenerator, serializerProvider);
    }

    public abstract void serialize(T t, JacksonBsonGenerator<T> generator, SerializerProvider provider)
            throws IOException;
}
