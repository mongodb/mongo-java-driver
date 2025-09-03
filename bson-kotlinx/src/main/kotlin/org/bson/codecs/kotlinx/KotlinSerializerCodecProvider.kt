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

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.modules.SerializersModule
import org.bson.codecs.Codec
import org.bson.codecs.configuration.CodecProvider
import org.bson.codecs.configuration.CodecRegistry

/**
 * A Kotlin Serialization based Codec Provider
 *
 * The underlying class must be annotated with the `@Serializable`.
 */
@OptIn(ExperimentalSerializationApi::class)
public class KotlinSerializerCodecProvider(
    private val serializersModule: SerializersModule = defaultSerializersModule,
    private val bsonConfiguration: BsonConfiguration = BsonConfiguration()
) : CodecProvider {

    override fun <T : Any> get(clazz: Class<T>, registry: CodecRegistry): Codec<T>? =
        KotlinSerializerCodec.create(clazz.kotlin, serializersModule, bsonConfiguration)
}
