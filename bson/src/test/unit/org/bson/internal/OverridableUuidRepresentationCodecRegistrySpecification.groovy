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

package org.bson.internal

import org.bson.BsonBinaryReader
import org.bson.BsonBinaryWriter
import org.bson.ByteBufNIO
import org.bson.UuidRepresentation
import org.bson.codecs.Codec
import org.bson.codecs.DecoderContext
import org.bson.codecs.EncoderContext
import org.bson.io.BasicOutputBuffer
import org.bson.io.ByteBufferBsonInput
import spock.lang.Specification

import java.nio.ByteBuffer


class OverridableUuidRepresentationCodecRegistrySpecification extends Specification {
    def 'should handle cycles'() {
        given:
        def registry = new OverridableUuidRepresentationCodecRegistry(new ClassModelCodecProvider(), UuidRepresentation.STANDARD)

        when:
        Codec<Top> topCodec = registry.get(Top)

        then:
        topCodec instanceof TopCodec

        when:
        def top = new Top('Bob',
                new Top('Jim', null, null),
                new Nested('George', new Top('Joe', null, null)))
        def writer = new BsonBinaryWriter(new BasicOutputBuffer())
        topCodec.encode(writer, top, EncoderContext.builder().build())
        ByteArrayOutputStream os = new ByteArrayOutputStream()
        writer.getBsonOutput().pipe(os)
        writer.close()

        then:
        topCodec.decode(new BsonBinaryReader(new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(os.toByteArray())))),
                DecoderContext.builder().build()) == top
    }
}
