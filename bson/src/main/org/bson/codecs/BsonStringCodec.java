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

import org.bson.BsonReader;
import org.bson.BsonString;
import org.bson.BsonWriter;

/**
 * A Codec for BsonString instances.
 *
 * @since 3.0
 */
public class BsonStringCodec implements Codec<BsonString> {
    @Override
    public BsonString decode(final BsonReader reader, final DecoderContext decoderContext) {
        return new BsonString(reader.readString());
    }

    @Override
    public void encode(final BsonWriter writer, final BsonString value, final EncoderContext encoderContext) {
        writer.writeString(value.getValue());
    }

    @Override
    public Class<BsonString> getEncoderClass() {
        return BsonString.class;
    }
}
