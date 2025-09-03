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

package org.bson.json;

import org.bson.BsonBinary;

import java.util.Base64;

class ExtendedJsonBinaryConverter implements Converter<BsonBinary> {

    @Override
    public void convert(final BsonBinary value, final StrictJsonWriter writer) {
        writer.writeStartObject();
        writer.writeStartObject("$binary");
        writer.writeString("base64", Base64.getEncoder().encodeToString(value.getData()));
        writer.writeString("subType", String.format("%02X", value.getType()));
        writer.writeEndObject();
        writer.writeEndObject();
    }
}
