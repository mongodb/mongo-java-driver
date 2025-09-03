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

import java.lang.reflect.Type
import org.bson.codecs.Codec
import org.bson.codecs.configuration.CodecProvider
import org.bson.codecs.configuration.CodecRegistry

/** A Kotlin reflection based Codec Provider for data classes */
public class DataClassCodecProvider : CodecProvider {
    override fun <T : Any> get(clazz: Class<T>, registry: CodecRegistry): Codec<T>? = get(clazz, emptyList(), registry)

    override fun <T : Any> get(clazz: Class<T>, typeArguments: List<Type>, registry: CodecRegistry): Codec<T>? =
        DataClassCodec.create(clazz.kotlin, registry, typeArguments)
}
