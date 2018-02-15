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

package org.bson;

import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.io.BsonInputMark;

import static org.bson.codecs.BsonValueCodecProvider.getClassForBsonType;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

final class RawBsonValueHelper {
    private static final CodecRegistry REGISTRY = fromProviders(new BsonValueCodecProvider());

    static BsonValue decode(final byte[] bytes, final BsonBinaryReader bsonReader) {
        if (bsonReader.getCurrentBsonType() == BsonType.DOCUMENT || bsonReader.getCurrentBsonType() == BsonType.ARRAY) {
            int position = bsonReader.getBsonInput().getPosition();
            BsonInputMark mark = bsonReader.getBsonInput().getMark(4);
            int size = bsonReader.getBsonInput().readInt32();
            mark.reset();
            bsonReader.skipValue();
            if (bsonReader.getCurrentBsonType() == BsonType.DOCUMENT) {
                return new RawBsonDocument(bytes, position, size);
            } else {
                return new RawBsonArray(bytes, position, size);
            }
        } else {
            return REGISTRY.get(getClassForBsonType(bsonReader.getCurrentBsonType())).decode(bsonReader, DecoderContext.builder().build());
        }
    }

    private RawBsonValueHelper() {
    }
}
