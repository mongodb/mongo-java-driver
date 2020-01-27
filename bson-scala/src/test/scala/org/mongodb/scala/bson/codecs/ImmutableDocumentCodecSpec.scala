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

import java.nio.ByteBuffer
import java.util.Date

import scala.collection.JavaConverters._

import org.bson._
import org.bson.codecs.configuration.CodecRegistry
import org.bson.codecs.{ DecoderContext, EncoderContext }
import org.bson.io.{ BasicOutputBuffer, ByteBufferBsonInput }
import org.bson.types.ObjectId

import org.mongodb.scala.bson.codecs.Registry.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.collection.immutable.Document
import org.scalatest.{ FlatSpec, Matchers }

class ImmutableDocumentCodecSpec extends FlatSpec with Matchers {

  val registry: CodecRegistry = DEFAULT_CODEC_REGISTRY

  "MutableDocumentCodec" should "encode and decode all default types with readers and writers" in {
    val original: Document = Document(
      "binary" -> new BsonBinary("bson".toCharArray map (_.toByte)),
      "boolean" -> new BsonBoolean(true),
      "dateTime" -> new BsonDateTime(new Date().getTime),
      "double" -> new BsonDouble(1.0),
      "int" -> new BsonInt32(1),
      "long" -> new BsonInt64(1L),
      "null" -> new BsonNull(),
      "objectId" -> new BsonObjectId(new ObjectId()),
      "regEx" -> new BsonRegularExpression("^bson".r.regex),
      "string" -> new BsonString("string"),
      "symbol" -> new BsonSymbol(Symbol("bson").name),
      "bsonDocument" -> new BsonDocument("a", new BsonString("string")),
      "array" -> new BsonArray(List(new BsonString("string"), new BsonBoolean(false)).asJava)
    )

    info("encoding")
    val writer: BsonBinaryWriter = new BsonBinaryWriter(new BasicOutputBuffer())
    ImmutableDocumentCodec(registry).encode(writer, original, EncoderContext.builder().build())

    info("decoding")
    val buffer: BasicOutputBuffer = writer.getBsonOutput().asInstanceOf[BasicOutputBuffer];
    val reader: BsonBinaryReader = new BsonBinaryReader(
      new ByteBufferBsonInput(
        new ByteBufNIO(ByteBuffer.wrap(buffer.toByteArray))
      )
    )

    val decodedDocument = ImmutableDocumentCodec().decode(reader, DecoderContext.builder().build())

    decodedDocument shouldBe a[Document]
    original should equal(decodedDocument)
  }

  it should "respect encodeIdFirst property in encoder context" in {
    val original: Document = Document(
      "a" -> new BsonString("string"),
      "_id" -> new BsonInt32(1),
      "nested" -> Document("a" -> new BsonString("string"), "_id" -> new BsonInt32(1)).toBsonDocument
    )

    info("encoding")
    val writer: BsonBinaryWriter = new BsonBinaryWriter(new BasicOutputBuffer())
    ImmutableDocumentCodec(registry).encode(
      writer,
      original,
      EncoderContext.builder().isEncodingCollectibleDocument(true).build()
    )

    info("decoding")
    val buffer: BasicOutputBuffer = writer.getBsonOutput().asInstanceOf[BasicOutputBuffer];
    val reader: BsonBinaryReader =
      new BsonBinaryReader(new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(buffer.toByteArray))))

    val decodedDocument = ImmutableDocumentCodec().decode(reader, DecoderContext.builder().build())

    decodedDocument shouldBe a[Document]
    original should equal(decodedDocument)
    decodedDocument.keys.toList should contain theSameElementsInOrderAs (List("_id", "a", "nested"))

    Document(decodedDocument[BsonDocument]("nested")).keys.toList should contain theSameElementsInOrderAs (List(
      "a",
      "_id"
    ))
  }

  it should "encoder class should work as expected" in {
    ImmutableDocumentCodec().getEncoderClass should equal(classOf[Document])
  }

  it should "determine if document has an _id" in {
    ImmutableDocumentCodec().documentHasId(Document()) should be(false)
    ImmutableDocumentCodec().documentHasId(Document("_id" -> new BsonInt32(1))) should be(true)
  }

  it should "get the document_id" in {
    ImmutableDocumentCodec().getDocumentId(Document()) should be(null)
    ImmutableDocumentCodec().getDocumentId(Document("_id" -> new BsonInt32(1))) should be(new BsonInt32(1))
  }

  it should "generate document id if absent but not mutate original document" in {
    val document = Document()
    val document2 = ImmutableDocumentCodec().generateIdIfAbsentFromDocument(document)
    document.contains("_id") shouldBe false
    document2("_id") shouldBe a[BsonObjectId]
  }

  it should "not generate document id if present" in {
    val document = Document("_id" -> new BsonInt32(1))
    ImmutableDocumentCodec().generateIdIfAbsentFromDocument(document)
    document("_id") should equal(new BsonInt32(1))
  }
}
