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

package org.mongodb.kotlin.id.serialization

import kotlinx.serialization.KSerializer
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.modules.SerializersModule
import org.mongodb.kotlin.id.Id
import org.mongodb.kotlin.id.IdGenerator
import org.mongodb.kotlin.id.StringId
import org.mongodb.kotlin.id.jvm.loadIdGeneratorProvider
import kotlin.reflect.KClass


/**
 * Provides [Id] kotlinx Serialization module.
 */
fun idKotlinxSerializationModule(generator: IdGenerator = loadIdGeneratorProvider().generator): SerializersModule =
    SerializersModule {
        contextual(Id::class, IdSerializer(generator))
        contextual(StringId::class, IdSerializer(generator))
        if (generator.idClass != StringId::class) {
            @Suppress("UNCHECKED_CAST")
            contextual(generator.idClass as KClass<Id<*>>, IdSerializer(generator))
        }
    }

private class IdSerializer<T : Id<*>>(val generator: IdGenerator) : KSerializer<T> {

    override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor("IdSerializer", PrimitiveKind.STRING)

    @Suppress("UNCHECKED_CAST")
    override fun deserialize(decoder: Decoder): T =
        generator.create(decoder.decodeString()) as T

    override fun serialize(encoder: Encoder, value: T) {
        encoder.encodeString(value.toString())
    }

}
