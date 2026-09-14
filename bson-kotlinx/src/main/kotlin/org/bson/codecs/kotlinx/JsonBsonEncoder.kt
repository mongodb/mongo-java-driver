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
package org.bson.codecs.kotlinx

import java.math.BigDecimal
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonEncoder
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.modules.SerializersModule
import org.bson.BsonWriter
import org.bson.codecs.kotlinx.utils.BsonCodecUtils.toJsonNamingStrategy
import org.bson.types.Decimal128

/**
 * A [BsonEncoderImpl] that also implements [JsonEncoder], so that a kotlinx.serialization [JsonElement] can be used as
 * a property of a `@Serializable` class and written straight to a [BsonWriter].
 *
 * A [JsonElement] tree is written as plain JSON rather than as Extended JSON, which limits how faithfully BSON types
 * survive it:
 * - **Numbers are typed from the literal's text.** JSON has a single number type, so a literal containing a fraction or
 *   an exponent becomes a BSON double and anything else becomes an int, long or `Decimal128`. See [encodeJsonPrimitive]
 *   for the details and for why a `BigDecimal` is not preserved.
 * - **Extended JSON is not interpreted.** A nested object such as `{"$oid": "..."}` is written literally, as a
 *   sub-document with a `$`-prefixed field name, not as the BSON type it denotes.
 * - **BSON type identity is not preserved across a round trip.** [JsonBsonDecoder] flattens each BSON type into a bare
 *   [JsonPrimitive] - an `ObjectId` becomes its hexadecimal string, a date-time becomes a number of milliseconds and a
 *   binary value becomes a Base64 or UUID string - so re-encoding a decoded [JsonElement] can yield a different BSON
 *   type from the one that was read.
 *
 * Declare a property with the type you need - `Decimal128`, `ObjectId`, `BsonValue` or one of its subtypes, all of
 * which are handled by [defaultSerializersModule] - when a BSON type has to be preserved exactly.
 */
@OptIn(ExperimentalSerializationApi::class)
internal class JsonBsonEncoder(
    writer: BsonWriter,
    override val serializersModule: SerializersModule,
    configuration: BsonConfiguration,
) : BsonEncoderImpl(writer, serializersModule, configuration), JsonEncoder {

    companion object {
        private val DOUBLE_MAX_VALUE = BigDecimal.valueOf(Double.MAX_VALUE)
        private val DOUBLE_MIN_VALUE = BigDecimal.valueOf(Double.MIN_VALUE)
        private val INT_MIN_VALUE = BigDecimal.valueOf(Int.MIN_VALUE.toLong())
        private val INT_MAX_VALUE = BigDecimal.valueOf(Int.MAX_VALUE.toLong())
        private val LONG_MIN_VALUE = BigDecimal.valueOf(Long.MIN_VALUE)
        private val LONG_MAX_VALUE = BigDecimal.valueOf(Long.MAX_VALUE)
        private const val MAX_MESSAGE_LITERAL_LENGTH = 64
        /** The JSON number grammar (RFC 8259, section 6). */
        private val JSON_NUMBER = Regex("""-?(0|[1-9]\d*)(\.\d+)?([eE][-+]?\d+)?""")
    }

    override val json = Json {
        explicitNulls = configuration.explicitNulls
        encodeDefaults = configuration.encodeDefaults
        classDiscriminator = configuration.classDiscriminator
        namingStrategy = configuration.bsonNamingStrategy.toJsonNamingStrategy()
        serializersModule = this@JsonBsonEncoder.serializersModule
    }

    override fun <T> encodeSerializableValue(serializer: SerializationStrategy<T>, value: T) {
        if (value is JsonElement) encodeJsonElement(value)
        else super<BsonEncoderImpl>.encodeSerializableValue(serializer, value)
    }

    override fun encodeJsonElement(element: JsonElement) {
        deferredElementHandler.with(
            {
                when (element) {
                    is JsonNull ->
                        if (configuration.explicitNulls) {
                            encodeName(it)
                            encodeNull()
                        }
                    is JsonPrimitive -> {
                        encodeName(it)
                        encodeJsonPrimitive(element)
                    }
                    is JsonObject -> {
                        encodeName(it)
                        encodeJsonObject(element)
                    }
                    is JsonArray -> {
                        encodeName(it)
                        encodeJsonArray(element)
                    }
                }
            },
            {
                when (element) {
                    is JsonNull -> if (configuration.explicitNulls) encodeNull()
                    is JsonPrimitive -> encodeJsonPrimitive(element)
                    is JsonObject -> encodeJsonObject(element)
                    is JsonArray -> encodeJsonArray(element)
                }
            })
    }

    /**
     * Encodes a JSON primitive as the closest BSON type.
     *
     * JSON defines a single number type, so `3` and `3.0` denote the same value and a numeric literal's text is the
     * only available signal of its type. A literal containing a fraction or an exponent is encoded as a BSON double,
     * matching how the driver's own JSON parser types numbers: `org.bson.json.JsonScanner` reads any literal with a `.`
     * or an exponent as a double. Within the range of a double, `{"a": 1e20}` therefore encodes to the same BSON type
     * through this codec as it does through `BsonDocument.parse`.
     *
     * Outside the range of the matching BSON type a literal widens to a `Decimal128` rather than losing data, which is
     * where this codec is deliberately more precise than `JsonScanner`: `1e400` and `1e-330` widen here, where
     * `BsonDocument.parse` yields `Infinity` and `0.0`, and `9223372036854775808` widens where `JsonScanner` throws.
     *
     * Subnormal literals such as `1e-320` are the exception, encoding as doubles and losing precision exactly as they
     * do in `JsonScanner`.
     *
     * Note: `kotlinx.serialization` has no `BigDecimal` support, so a `BigDecimal` placed in a `JsonObject` is stored
     * as the string from its `toString()` and cannot be distinguished from a hand-written literal.
     * `BigDecimal("1E+19")` consequently encodes as a BSON double, not a `Decimal128`. Use `Decimal128`, or `BsonValue`
     * with `BsonValueSerializer`, when the BSON type must be preserved exactly.
     */
    private fun encodeJsonPrimitive(primitive: JsonPrimitive) {
        val content = primitive.content
        when {
            primitive.isString -> encodeString(content)
            content == "true" || content == "false" -> encodeBoolean(content.toBooleanStrict())
            else -> {
                val decimal = parseNumericLiteral(content)
                when {
                    isFloatingLiteral(content) -> {
                        val abs = decimal.abs()
                        if ((decimal.signum() == 0 || abs >= DOUBLE_MIN_VALUE) && abs <= DOUBLE_MAX_VALUE) {
                            encodeDouble(parseDouble(content))
                        } else {
                            encodeDecimal128(content, decimal)
                        }
                    }
                    INT_MIN_VALUE <= decimal && decimal <= INT_MAX_VALUE -> encodeInt(decimal.toInt())
                    LONG_MIN_VALUE <= decimal && decimal <= LONG_MAX_VALUE -> encodeLong(decimal.toLong())
                    else -> encodeDecimal128(content, decimal)
                }
            }
        }
    }

    /** Determines whether a numeric literal was written with a fraction or an exponent. */
    private fun isFloatingLiteral(content: String): Boolean = content.any { it == '.' || it == 'e' || it == 'E' }

    /**
     * Parses a non-string, non-boolean JSON literal as a [BigDecimal].
     *
     * The content is not guaranteed to be a JSON number: kotlinx.serialization does not validate a
     * `JsonUnquotedLiteral`, and permits `NaN` and `Infinity` when `allowSpecialFloatingPointValues` is enabled.
     *
     * The literal is matched against the JSON number grammar before parsing because `BigDecimal` is more lenient than
     * JSON: it accepts any Unicode decimal digit, a leading `+`, a leading `.` and a trailing `.`. Without the check
     * `"١٢٣"`, `"+1"`, `".5"` and `"1."` would all be encoded as numbers, while `"١.٥"` was rejected, since only the
     * latter also fails to parse as a [Double]. The grammar is the one accepted by [org.bson.json.JsonScanner], so a
     * literal is typed here only if the driver's own JSON parser would also read it as a number.
     */
    private fun parseNumericLiteral(content: String): BigDecimal {
        if (!JSON_NUMBER.matches(content)) throw notANumber(content)
        return try {
            BigDecimal(content)
        } catch (e: NumberFormatException) {
            throw notANumber(content, e)
        }
    }

    /**
     * Parses a floating-point literal as a [Double].
     *
     * Parsing the literal rather than converting the [BigDecimal] preserves `-0.0`, which `BigDecimal` cannot
     * represent.
     */
    private fun parseDouble(content: String): Double = content.toDoubleOrNull() ?: throw notANumber(content)

    /**
     * Encodes a numeric literal that cannot be represented as a BSON int, long or double.
     *
     * `Decimal128` holds at most 34 significant digits and will not round, so report an unrepresentable literal rather
     * than letting a bare `NumberFormatException` escape the codec.
     */
    private fun encodeDecimal128(content: String, decimal: BigDecimal) {
        val decimal128 =
            try {
                Decimal128(decimal)
            } catch (e: NumberFormatException) {
                throw SerializationException(
                    "Cannot encode the JSON number '${abbreviate(content)}': " +
                        "its range or precision exceeds BSON Decimal128.",
                    e)
            }
        writer.writeDecimal128(decimal128)
    }

    private fun notANumber(content: String, cause: NumberFormatException? = null): SerializationException =
        SerializationException("Cannot encode '${abbreviate(content)}' as BSON: it is not a valid JSON number.", cause)

    /** Keeps a literal quoted in an exception message from being unbounded, as it is user supplied. */
    private fun abbreviate(content: String): String =
        if (content.length <= MAX_MESSAGE_LITERAL_LENGTH) content else content.take(MAX_MESSAGE_LITERAL_LENGTH) + "..."

    private fun encodeJsonObject(obj: JsonObject) {
        writer.writeStartDocument()
        obj.forEach { k, v ->
            deferredElementHandler.set(k)
            encodeJsonElement(v)
        }
        writer.writeEndDocument()
    }

    private fun encodeJsonArray(array: JsonArray) {
        writer.writeStartArray()
        array.forEach(::encodeJsonElement)
        writer.writeEndArray()
    }
}
