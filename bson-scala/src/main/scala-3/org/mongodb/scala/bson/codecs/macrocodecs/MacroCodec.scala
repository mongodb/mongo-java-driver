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

package org.mongodb.scala.bson.codecs.macrocodecs

import scala.jdk.CollectionConverters._
import scala.collection.mutable

import org.bson._
import org.bson.codecs.configuration.{ CodecRegistries, CodecRegistry }
import org.bson.codecs.{ Codec, DecoderContext, Encoder, EncoderContext }
import scala.collection.immutable.Vector
import java.util.concurrent.ConcurrentHashMap

import org.mongodb.scala.bson.BsonNull

/**
 *
 * @tparam T the case class type for the codec
 * @since 2.0
 */
trait MacroCodec[T] extends Codec[T] {

  /**
   * Creates a `Map[String, Class[?]]` mapping the case class name and the type.
   */
  val caseClassesMap: Map[String, Class[?]]

  /**
   * Creates a `Map[Class[?], Boolean]` mapping field types to a boolean representing if they are a case class.
   */
  val classToCaseClassMap: Map[Class[?], Boolean]

  /**
   * A nested map of case class name to a Map of the given field names and a list of the field types.
   */
  val classFieldTypeArgsMap: Map[String, Map[String, List[Class[?]]]]

  /**
   * The case class type for the codec.
   */
  val encoderClass: Class[T]

  /**
   * The `CodecRegistry` for use with the codec.
   */
  val codecRegistry: CodecRegistry

  /**
   * Creates a new instance of the case class with the provided data
   *
   * @param className the name of the class to be instantiated
   * @param fieldsData the Map of data for the class
   * @return the new instance of the class
   */
  def getInstance(className: String, fieldsData: Map[String, Any]): T

  /**
   * The method that writes the data for the case class
   *
   * @param className the name of the current case class being written
   * @param writer the `BsonWriter`
   * @param value the value to the case class
   * @param encoderContext the `EncoderContext`
   */
  def writeCaseClassData(className: String, writer: BsonWriter, value: T, encoderContext: EncoderContext): Unit

  /**
   * The BSON discriminator field name used to identify the concrete type when encoding/decoding sealed class hierarchies.
   * When a sealed trait or class has multiple subclasses, each encoded document includes a `_t` field whose value is
   * the simple class name (e.g., `{"_t": "SealedClassA", ...}`). This follows MongoDB's convention for polymorphic types.
   */
  val classFieldName = "_t"

  /**
   * True when the codec handles a sealed hierarchy (multiple concrete types), meaning the `_t` discriminator
   * field must be written/read. False when encoding a single concrete case class directly.
   */
  lazy val hasClassFieldName: Boolean = caseClassesMapInv.keySet != Set(encoderClass)
  lazy val caseClassesMapInv: Map[Class[?], String] = caseClassesMap.map(_.swap)
  protected lazy val registry: CodecRegistry =
    CodecRegistries.fromRegistries(List(codecRegistry, CodecRegistries.fromCodecs(this)).asJava)

  /** Thread-safe cache for codec lookups, avoiding repeated registry traversal for the same class. */
  private val codecCache = new ConcurrentHashMap[Class[?], Codec[?]]()

  /** Returns a cached codec for the given class, looking it up in the registry on first access. */
  protected def getCachedCodec[V](clazz: Class[V]): Codec[V] =
    codecCache.computeIfAbsent(clazz, _ => registry.get(clazz)).asInstanceOf[Codec[V]]

  protected val bsonNull = BsonNull()

  override def encode(writer: BsonWriter, value: T, encoderContext: EncoderContext): Unit = {
    if (value == null) { // scalastyle:ignore
      throw new BsonInvalidOperationException(s"Invalid value for $encoderClass found a `null` value.")
    }
    writeValue(writer, value, encoderContext)
  }

  override def decode(reader: BsonReader, decoderContext: DecoderContext): T = {
    val className = getClassName(reader, decoderContext)
    val fieldTypeArgsMap = classFieldTypeArgsMap(className)
    val map = mutable.Map[String, Any]()
    reader.readStartDocument()
    while (reader.readBsonType ne BsonType.END_OF_DOCUMENT) {
      val name = reader.readName
      val typeArgs = if (name == classFieldName) List(classOf[String]) else fieldTypeArgsMap.getOrElse(name, List.empty)
      if (typeArgs.isEmpty) {
        reader.skipValue()
      } else {
        map += (name -> readValue(reader, decoderContext, typeArgs.head, typeArgs.tail))
      }
    }
    reader.readEndDocument()
    getInstance(className, map.toMap)
  }

  override def getEncoderClass: Class[T] = encoderClass

  protected def getClassName(reader: BsonReader, decoderContext: DecoderContext): String = {
    if (hasClassFieldName) {
      // Find the class name
      @scala.annotation.tailrec
      def readOptionalClassName(): Option[String] = {
        if (reader.readBsonType == BsonType.END_OF_DOCUMENT) {
          None
        } else if (reader.readName == classFieldName) {
          Some(codecRegistry.get(classOf[String]).decode(reader, decoderContext))
        } else {
          reader.skipValue()
          readOptionalClassName()
        }
      }

      val mark: BsonReaderMark = reader.getMark()
      reader.readStartDocument()
      val optionalClassName: Option[String] = readOptionalClassName()
      mark.reset()

      val className = optionalClassName.getOrElse {
        throw new BsonInvalidOperationException(s"Could not decode sealed case class. Missing '$classFieldName' field.")
      }

      if (!caseClassesMap.contains(className)) {
        throw new BsonInvalidOperationException(s"Could not decode sealed case class, unknown class $className.")
      }
      className
    } else {
      caseClassesMap.head._1
    }
  }

  def writeClassFieldName(writer: BsonWriter, className: String, encoderContext: EncoderContext): Unit = {
    if (hasClassFieldName) {
      writer.writeName(classFieldName)
      this.writeValue(writer, className, encoderContext)
    }
  }

  def throwMissingField(fieldName: String): Nothing =
    throw new BsonInvalidOperationException(s"Missing field: $fieldName")

  def throwUnexpectedClass(className: String): Nothing =
    throw new BsonInvalidOperationException(s"Unexpected class type: $className")

  def writeFieldValue[V](
                                        fieldName: String,
                                        writer: BsonWriter,
                                        value: V,
                                        encoderContext: EncoderContext
                                      ): Unit = {
    if (value == null) { // scalastyle:ignore
      throw new BsonInvalidOperationException(s"Invalid value for $fieldName found a `null` value.")
    }
    writeValue(writer, value, encoderContext)
  }

  protected def writeValue[V](writer: BsonWriter, value: V, encoderContext: EncoderContext): Unit = {
    val clazz = value.getClass
    caseClassesMapInv.get(clazz) match {
      case Some(className) =>
        writeCaseClassData(className: String, writer: BsonWriter, value.asInstanceOf[T], encoderContext: EncoderContext)
      case None =>
        val codec = getCachedCodec(clazz).asInstanceOf[Encoder[V]]
        encoderContext.encodeWithChildContext(codec, writer, value)
    }
  }

  protected def readValue[V](
                              reader: BsonReader,
                              decoderContext: DecoderContext,
                              clazz: Class[V],
                              typeArgs: List[Class[?]]
                            ): V = {
    val currentType = reader.getCurrentBsonType
    currentType match {
      case BsonType.DOCUMENT => readDocument(reader, decoderContext, clazz, typeArgs)
      case BsonType.ARRAY    => readArray(reader, decoderContext, clazz, typeArgs)
      case BsonType.NULL =>
        reader.readNull()
        null.asInstanceOf[V] // scalastyle:ignore
      case _ => getCachedCodec(clazz).decode(reader, decoderContext)
    }
  }

  protected def readArray[V](
                              reader: BsonReader,
                              decoderContext: DecoderContext,
                              clazz: Class[V],
                              typeArgs: List[Class[?]]
                            ): V = {

    if (typeArgs.isEmpty) {
      throw new BsonInvalidOperationException(
        s"Invalid Bson format for '${clazz.getSimpleName}'. Found a list but there is no type data."
      )
    }
    reader.readStartArray()
    val list = mutable.ListBuffer[Any]()
    while (reader.readBsonType ne BsonType.END_OF_DOCUMENT) {
      list.append(readValue(reader, decoderContext, typeArgs.head, typeArgs.tail))
    }
    reader.readEndArray()
    if (classOf[Set[?]].isAssignableFrom(clazz)) {
      list.toSet.asInstanceOf[V]
    } else if (classOf[Vector[?]].isAssignableFrom(clazz)) {
      list.toVector.asInstanceOf[V]
    } else if (classOf[LazyList[?]].isAssignableFrom(clazz)) {
      LazyList.from(list).asInstanceOf[V]
    } else {
      list.toList.asInstanceOf[V]
    }
  }

  protected def readDocument[V](
                                 reader: BsonReader,
                                 decoderContext: DecoderContext,
                                 clazz: Class[V],
                                 typeArgs: List[Class[?]]
                               ): V = {
    // Delegate to the registry in two cases:
    // 1. The type is a case class / sealed type — its own MacroCodec handles decoding
    // 2. No type args available (e.g., BsonDocument) — we can't decode fields ourselves
    if (classToCaseClassMap.getOrElse(clazz, false) || typeArgs.isEmpty) {
      getCachedCodec(clazz).decode(reader, decoderContext)
    } else {
      val map = mutable.Map[String, Any]()
      reader.readStartDocument()
      while (reader.readBsonType ne BsonType.END_OF_DOCUMENT) {
        val name = reader.readName
        map += (name -> readValue(reader, decoderContext, typeArgs.head, typeArgs.tail))
      }
      reader.readEndDocument()
      map.toMap.asInstanceOf[V]
    }
  }
}
