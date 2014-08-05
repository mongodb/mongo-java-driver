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

import com.fasterxml.jackson.databind.SerializerProvider;
import org.bson.types.ObjectId;

import java.io.IOException;

/**
 * Created by guo on 7/29/14.
 */
public class JacksonObjectIdSerializer extends JacksonBsonSerializer<ObjectId> {


    @Override
    public void serialize(ObjectId value, JacksonBsonGenerator<ObjectId> generator, SerializerProvider provider) throws IOException {
        if (value == null) {
            provider.defaultSerializeNull(generator);
        } else {
            generator.writeObjectId(value);
        }
    }
}
