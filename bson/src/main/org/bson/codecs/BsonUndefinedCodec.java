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
import org.bson.BsonUndefined;
import org.bson.BsonWriter;

/**
 * <p>Allows reading and writing of the BSON Undefined type.  On encoding, it will write the correct type to the BsonWriter, but ignore the
 * value, and on decoding it will read the type off the BsonReader and return an Undefined type, which simply represents a placeholder for
 * the undefined value.</p>
 *
 * <p>The undefined type is deprecated (see the spec).</p>
 *
 * @see <a href="http://bsonspec.org/spec.html">BSON Spec</a>
 * @see org.bson.BsonType#UNDEFINED
 * @since 3.0
 */
public class BsonUndefinedCodec implements Codec<BsonUndefined> {
    @Override
    public BsonUndefined decode(final BsonReader reader, final DecoderContext decoderContext) {
        reader.readUndefined();
        return new BsonUndefined();
    }

    @Override
    public void encode(final BsonWriter writer, final BsonUndefined value, final EncoderContext encoderContext) {
        writer.writeUndefined();
    }

    @Override
    public Class<BsonUndefined> getEncoderClass() {
        return BsonUndefined.class;
    }
}
