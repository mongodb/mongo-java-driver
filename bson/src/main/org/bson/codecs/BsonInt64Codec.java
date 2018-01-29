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

package org.bson.codecs;

import org.bson.BsonInt64;
import org.bson.BsonReader;
import org.bson.BsonWriter;

/**
 * A Codec for BsonInt64 instances.
 *
 * @since 3.0
 */
public class BsonInt64Codec implements Codec<BsonInt64> {
    @Override
    public BsonInt64 decode(final BsonReader reader, final DecoderContext decoderContext) {
        return new BsonInt64(reader.readInt64());
    }

    @Override
    public void encode(final BsonWriter writer, final BsonInt64 value, final EncoderContext encoderContext) {
        writer.writeInt64(value.getValue());
    }

    @Override
    public Class<BsonInt64> getEncoderClass() {
        return BsonInt64.class;
    }
}
