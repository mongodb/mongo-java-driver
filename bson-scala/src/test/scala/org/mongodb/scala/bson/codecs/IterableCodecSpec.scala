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

package org.mongodb.scala.bson.codecs

import org.bson.codecs.{ DecoderContext, EncoderContext }
import org.bson.{ BsonDocumentReader, BsonDocumentWriter, Transformer }
import org.mongodb.scala.bson.codecs.Registry.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.{ BaseSpec, BsonDocument }

class IterableCodecSpec extends BaseSpec {

  "IterableCodec" should "have the correct encoding class" in {
    val codec = IterableCodec(DEFAULT_CODEC_REGISTRY, BsonTypeClassMap())
    codec.getEncoderClass() should equal(classOf[Iterable[_]])
  }

  it should "encode an Iterable to a BSON array" in {
    val codec = IterableCodec(DEFAULT_CODEC_REGISTRY, BsonTypeClassMap())
    val writer = new BsonDocumentWriter(new BsonDocument())

    writer.writeStartDocument()
    writer.writeName("array")
    codec.encode(writer, List(1, 2, 3), EncoderContext.builder().build())
    writer.writeEndDocument()
    writer.getDocument should equal(BsonDocument("{array : [1, 2, 3]}"))
  }

  it should "decode a BSON array to an Iterable" in {
    val codec = IterableCodec(DEFAULT_CODEC_REGISTRY, BsonTypeClassMap())
    val reader = new BsonDocumentReader(BsonDocument("{array : [1, 2, 3]}"))

    reader.readStartDocument()
    reader.readName("array")
    val iterable = codec.decode(reader, DecoderContext.builder().build())
    reader.readEndDocument()

    iterable should equal(List(1, 2, 3))
  }

  it should "encode an Iterable containing Maps to a BSON array" in {
    val codec = IterableCodec(DEFAULT_CODEC_REGISTRY, BsonTypeClassMap())
    val writer = new BsonDocumentWriter(new BsonDocument())

    writer.writeStartDocument()
    writer.writeName("array")
    codec.encode(writer, List(Map("a" -> 1, "b" -> 2, "c" -> null)), EncoderContext.builder().build()) // scalastyle:ignore
    writer.writeEndDocument()
    writer.getDocument should equal(BsonDocument("{array : [{a: 1, b: 2, c: null}]}"))
  }

  it should "decode a BSON array containing maps to an Iterable" in {
    val codec = IterableCodec(DEFAULT_CODEC_REGISTRY, BsonTypeClassMap())
    val reader = new BsonDocumentReader(BsonDocument("{array : [{a: 1, b: 2, c: null}]}"))

    reader.readStartDocument()
    reader.readName("array")
    val iterable = codec.decode(reader, DecoderContext.builder().build())
    reader.readEndDocument()

    iterable should equal(List(Map("a" -> 1, "b" -> 2, "c" -> null))) // scalastyle:ignore
  }

  it should "encode a Map to a BSON document" in {
    val codec = IterableCodec(DEFAULT_CODEC_REGISTRY, BsonTypeClassMap())
    val writer = new BsonDocumentWriter(new BsonDocument())

    writer.writeStartDocument()
    writer.writeName("document")
    codec.encode(writer, Map("a" -> 1, "b" -> 2), EncoderContext.builder().build())
    writer.writeEndDocument()
    writer.getDocument should equal(BsonDocument("{document : {a: 1, b: 2}}"))
  }

  it should "decode a BSON Document to a Map" in {
    val codec = IterableCodec(DEFAULT_CODEC_REGISTRY, BsonTypeClassMap())
    val reader = new BsonDocumentReader(BsonDocument("{document : {a: 1, b: 2}}"))

    reader.readStartDocument()
    reader.readName("document")
    val iterable = codec.decode(reader, DecoderContext.builder().build())
    reader.readEndDocument()

    iterable should equal(Map("a" -> 1, "b" -> 2))
  }

  it should "use the provided transformer" in {
    val codec = IterableCodec(DEFAULT_CODEC_REGISTRY, BsonTypeClassMap(), new Transformer {
      override def transform(objectToTransform: Any): AnyRef = s"$objectToTransform"
    })
    val reader = new BsonDocumentReader(BsonDocument("{array : [1, 2, 3]}"))

    reader.readStartDocument()
    reader.readName("array")
    val iterable = codec.decode(reader, DecoderContext.builder().build())
    reader.readEndDocument()

    iterable.toList should contain theSameElementsInOrderAs List("1", "2", "3")
  }

}
