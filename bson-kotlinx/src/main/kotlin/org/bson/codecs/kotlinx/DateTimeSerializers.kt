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

import java.time.ZoneOffset
import kotlinx.datetime.Instant
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.LocalTime
import kotlinx.datetime.TimeZone
import kotlinx.datetime.UtcOffset
import kotlinx.datetime.atDate
import kotlinx.datetime.atStartOfDayIn
import kotlinx.datetime.toInstant
import kotlinx.datetime.toLocalDateTime
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.plus
import org.bson.BsonDateTime
import org.bson.codecs.kotlinx.utils.SerializationModuleUtils.isClassAvailable

/**
 * The default serializers module
 *
 * Handles:
 * - ObjectId serialization
 * - BsonValue serialization
 * - Instant serialization
 * - LocalDate serialization
 * - LocalDateTime serialization
 * - LocalTime serialization
 */
@ExperimentalSerializationApi
public val dateTimeSerializersModule: SerializersModule by lazy {
    var module = SerializersModule {}
    if (isClassAvailable("kotlinx.datetime.Instant")) {
        module += InstantAsBsonDateTime.serializersModule +
                LocalDateAsBsonDateTime.serializersModule +
                LocalDateTimeAsBsonDateTime.serializersModule +
                LocalTimeAsBsonDateTime.serializersModule
    }
    module
}

/**
 * Instant KSerializer.
 *
 * Encodes and decodes `Instant` objects to and from `BsonDateTime`. Data is extracted via
 * [kotlinx.datetime.Instant.fromEpochMilliseconds] and stored to millisecond accuracy.
 *
 * @since 5.2
 */
@ExperimentalSerializationApi
public object InstantAsBsonDateTime : KSerializer<Instant> {
    override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor("InstantAsBsonDateTime", PrimitiveKind.STRING)

    override fun serialize(encoder: Encoder, value: Instant) {
        when (encoder) {
            is BsonEncoder -> encoder.encodeBsonValue(BsonDateTime(value.toEpochMilliseconds()))
            else -> throw SerializationException("Instant is not supported by ${encoder::class}")
        }
    }

    override fun deserialize(decoder: Decoder): Instant {
        return when (decoder) {
            is BsonDecoder -> Instant.fromEpochMilliseconds(decoder.decodeBsonValue().asDateTime().value)
            else -> throw SerializationException("Instant is not supported by ${decoder::class}")
        }
    }

    @Suppress("UNCHECKED_CAST")
    public val serializersModule: SerializersModule = SerializersModule {
        contextual(Instant::class, InstantAsBsonDateTime as KSerializer<Instant>)
    }
}

/**
 * LocalDate KSerializer.
 *
 * Encodes and decodes `LocalDate` objects to and from `BsonDateTime`.
 *
 * Converts the `LocalDate` values to and from `UTC`.
 *
 * @since 5.2
 */
@ExperimentalSerializationApi
public object LocalDateAsBsonDateTime : KSerializer<LocalDate> {
    override val descriptor: SerialDescriptor =
        PrimitiveSerialDescriptor("LocalDateAsBsonDateTime", PrimitiveKind.STRING)

    override fun serialize(encoder: Encoder, value: LocalDate) {
        when (encoder) {
            is BsonEncoder -> {
                val epochMillis = value.atStartOfDayIn(TimeZone.UTC).toEpochMilliseconds()
                encoder.encodeBsonValue(BsonDateTime(epochMillis))
            }
            else -> throw SerializationException("LocalDate is not supported by ${encoder::class}")
        }
    }

    override fun deserialize(decoder: Decoder): LocalDate {
        return when (decoder) {
            is BsonDecoder ->
                Instant.fromEpochMilliseconds(decoder.decodeBsonValue().asDateTime().value)
                    .toLocalDateTime(TimeZone.UTC)
                    .date
            else -> throw SerializationException("LocalDate is not supported by ${decoder::class}")
        }
    }

    @Suppress("UNCHECKED_CAST")
    public val serializersModule: SerializersModule = SerializersModule {
        contextual(LocalDate::class, LocalDateAsBsonDateTime as KSerializer<LocalDate>)
    }
}

/**
 * LocalDateTime KSerializer.
 *
 * Encodes and decodes `LocalDateTime` objects to and from `BsonDateTime`. Data is stored to millisecond
 * accuracy.
 *
 * Converts the `LocalDateTime` values to and from `UTC`.
 *
 * @since 5.2
 */
@ExperimentalSerializationApi
public object LocalDateTimeAsBsonDateTime : KSerializer<LocalDateTime> {
    override val descriptor: SerialDescriptor =
        PrimitiveSerialDescriptor("LocalDateTimeAsBsonDateTime", PrimitiveKind.STRING)

    override fun serialize(encoder: Encoder, value: LocalDateTime) {
        when (encoder) {
            is BsonEncoder -> {
                val epochMillis = value.toInstant(UtcOffset(ZoneOffset.UTC)).toEpochMilliseconds()
                encoder.encodeBsonValue(BsonDateTime(epochMillis))
            }
            else -> throw SerializationException("LocalDateTime is not supported by ${encoder::class}")
        }
    }

    override fun deserialize(decoder: Decoder): LocalDateTime {
        return when (decoder) {
            is BsonDecoder ->
                Instant.fromEpochMilliseconds(decoder.decodeBsonValue().asDateTime().value)
                    .toLocalDateTime(TimeZone.UTC)
            else -> throw SerializationException("LocalDateTime is not supported by ${decoder::class}")
        }
    }

    @Suppress("UNCHECKED_CAST")
    public val serializersModule: SerializersModule = SerializersModule {
        contextual(LocalDateTime::class, LocalDateTimeAsBsonDateTime as KSerializer<LocalDateTime>)
    }
}

/**
 * LocalTime KSerializer.
 *
 * Encodes and decodes `LocalTime` objects to and from `BsonDateTime`. Data is stored to millisecond
 * accuracy.
 *
 * Converts the `LocalTime` values to and from EpochDay at `UTC`.
 *
 * @since 5.2
 */
@ExperimentalSerializationApi
public object LocalTimeAsBsonDateTime : KSerializer<LocalTime> {
    override val descriptor: SerialDescriptor =
        PrimitiveSerialDescriptor("LocalTimeAsBsonDateTime", PrimitiveKind.STRING)

    override fun serialize(encoder: Encoder, value: LocalTime) {
        when (encoder) {
            is BsonEncoder -> {
                val epochMillis =
                    value.atDate(LocalDate.fromEpochDays(0)).toInstant(UtcOffset(ZoneOffset.UTC)).toEpochMilliseconds()
                encoder.encodeBsonValue(BsonDateTime(epochMillis))
            }
            else -> throw SerializationException("LocalTime is not supported by ${encoder::class}")
        }
    }

    override fun deserialize(decoder: Decoder): LocalTime {
        return when (decoder) {
            is BsonDecoder ->
                Instant.fromEpochMilliseconds(decoder.decodeBsonValue().asDateTime().value)
                    .toLocalDateTime(TimeZone.UTC)
                    .time
            else -> throw SerializationException("LocalTime is not supported by ${decoder::class}")
        }
    }

    @Suppress("UNCHECKED_CAST")
    public val serializersModule: SerializersModule = SerializersModule {
        contextual(LocalTime::class, LocalTimeAsBsonDateTime as KSerializer<LocalTime>)
    }
}
