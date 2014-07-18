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

package com.mongodb.codecs;

import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonReader;
import org.bson.codecs.Decoder;
import org.bson.codecs.DecoderContext;
import org.bson.types.Binary;
import org.mongodb.BinaryTransformer;

import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("rawtypes")
public class TransformingBinaryDecoder implements Decoder<Object> {
    private final Map<Byte, BinaryTransformer> subTypeTransformerMap;

    public TransformingBinaryDecoder() {
        subTypeTransformerMap = new HashMap<Byte, BinaryTransformer>();
        subTypeTransformerMap.put(BsonBinarySubType.BINARY.getValue(), new BinaryToByteArrayTransformer());
        subTypeTransformerMap.put(BsonBinarySubType.OLD_BINARY.getValue(), new BinaryToByteArrayTransformer());
        subTypeTransformerMap.put(BsonBinarySubType.UUID_LEGACY.getValue(), new BinaryToUUIDTransformer());
    }

    @Override
    public Object decode(final BsonReader reader, final DecoderContext decoderContext) {
        BsonBinary binary = reader.readBinaryData();
        BinaryTransformer transformer = subTypeTransformerMap.get(binary.getType());
        if (transformer == null) {
            return new Binary(binary.getType(), binary.getData());
        } else {
            return transformer.transform(binary);
        }
    }
}
