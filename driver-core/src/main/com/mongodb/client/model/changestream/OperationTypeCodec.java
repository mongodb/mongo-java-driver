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

package com.mongodb.client.model.changestream;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

final class OperationTypeCodec implements Codec<OperationType> {

    @Override
    public OperationType decode(final BsonReader reader, final DecoderContext decoderContext) {
        return OperationType.fromString(reader.readString());
    }

    @Override
    public void encode(final BsonWriter writer, final OperationType value, final EncoderContext encoderContext) {
        writer.writeString(value.getValue());
    }

    @Override
    public Class<OperationType> getEncoderClass() {
        return OperationType.class;
    }
}
