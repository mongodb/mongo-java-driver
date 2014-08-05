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

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import org.bson.BsonType;

import java.io.IOException;
import java.util.Date;

/**
 * Created by guo on 7/30/14.
 */
class JacksonDateDeserializer extends JacksonBsonDeserializer<Date> {

    @Override
    public Date deserialize(JacksonBsonParser parser, DeserializationContext ctxt)
            throws IOException {
        if (parser.getCurrentToken() != JsonToken.VALUE_EMBEDDED_OBJECT ||
                parser.getCurBsonType() != BsonType.DATE_TIME) {
            throw ctxt.mappingException(Date.class);
        }

        Object obj = parser.getEmbeddedObject();
        if (obj == null) {
            return null;
        }
        return (Date)obj;
    }
}
