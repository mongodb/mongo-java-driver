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
import org.bson.BsonTimestamp;

import java.io.IOException;

/**
 * Created by guo on 8/1/14.
 */
class JacksonTimestampSerializer extends JacksonBsonSerializer<BsonTimestamp> {
    @Override
    public void serialize(BsonTimestamp timestamp, JacksonBsonGenerator<BsonTimestamp> bsonGenerator, SerializerProvider provider) throws IOException {
        if (timestamp == null) {
            provider.defaultSerializeNull(bsonGenerator);
        } else {
            bsonGenerator.writeTimestamp(timestamp);
        }
    }
}
