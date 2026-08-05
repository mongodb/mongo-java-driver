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
package org.bson.codecs.kotlin

import java.io.ByteArrayOutputStream
import org.bson.BsonInvalidOperationException
import org.bson.BsonReader
import org.bson.BsonType
import org.bson.codecs.ByteArrayCodec
import org.bson.codecs.DecoderContext

/**
 * A [ByteArrayCodec] that also decodes the legacy BSON Array representation of a `ByteArray`.
 *
 * Versions 5.2.0 - 5.9.x encoded `ByteArray` data class fields as a BSON Array of Int32, one
 * element per byte, rather than as a compact BSON Binary. This codec still decodes those documents, so data written by
 * those versions remains readable.
 *
 * Encoding is inherited unchanged from [ByteArrayCodec] and always produces BSON Binary. Re-saving a document therefore
 * migrates it away from the legacy representation.
 *
 * Only values the legacy encoder could have produced are accepted: every element must be an Int32 within the signed
 * byte range. Anything else throws [BsonInvalidOperationException] rather than silently decoding an unrelated BSON
 * Array into bytes.
 */
internal object LenientByteArrayCodec : ByteArrayCodec() {

    override fun decode(reader: BsonReader, decoderContext: DecoderContext): ByteArray =
        if (reader.currentBsonType == BsonType.ARRAY) {
            decodeLegacyArray(reader)
        } else {
            super.decode(reader, decoderContext)
        }

    private fun decodeLegacyArray(reader: BsonReader): ByteArray {
        val bytes = ByteArrayOutputStream()
        reader.readStartArray()
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            if (reader.currentBsonType != BsonType.INT32) {
                throw invalidElement(reader.currentBsonType.toString())
            }
            val value = reader.readInt32()
            if (value < Byte.MIN_VALUE || value > Byte.MAX_VALUE) {
                throw invalidElement(value.toString())
            }
            bytes.write(value)
        }
        reader.readEndArray()
        return bytes.toByteArray()
    }

    private fun invalidElement(found: String) =
        BsonInvalidOperationException(
            "Invalid element while decoding a byte array from a BSON Array: " +
                "expected an INT32 in the range -128..127, but found $found.")
}
