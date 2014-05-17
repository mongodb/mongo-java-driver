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

package org.bson.codecs;

import org.bson.BSONReader;
import org.bson.BSONWriter;
import org.bson.types.BSONTimestamp;

/**
 * Knows how to encode and decode BSON timestamps.
 */
public class BSONTimestampCodec implements Codec<BSONTimestamp> {
    @Override
    public void encode(final BSONWriter writer, final BSONTimestamp value) {
        writer.writeTimestamp(value);
    }

    @Override
    public BSONTimestamp decode(final BSONReader reader) {
        return reader.readTimestamp();
    }

    @Override
    public Class<BSONTimestamp> getEncoderClass() {
        return BSONTimestamp.class;
    }
}
