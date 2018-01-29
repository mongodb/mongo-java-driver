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
import org.bson.BsonTimestamp;
import org.bson.BsonWriter;

/**
 * A Codec for BSON Timestamp instances.
 *
 * @since 3.0
 */
public class BsonTimestampCodec implements Codec<BsonTimestamp> {
    @Override
    public void encode(final BsonWriter writer, final BsonTimestamp value, final EncoderContext encoderContext) {
        writer.writeTimestamp(value);
    }

    @Override
    public BsonTimestamp decode(final BsonReader reader, final DecoderContext decoderContext) {
        return reader.readTimestamp();
    }

    @Override
    public Class<BsonTimestamp> getEncoderClass() {
        return BsonTimestamp.class;
    }}
