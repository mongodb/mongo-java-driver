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

import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.mongodb.kotlin.id.Id
import org.mongodb.kotlin.id.jvm.newId
import org.mongodb.kotlin.id.serialization.idKotlinxSerializationModule
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 *
 */
class IdModuleTest {

    @Serializable
    data class Data(@Contextual val id: Id<Data> = newId())

    @Test
    fun testSerializationAndDeserialization() {
        val json = Json {
            serializersModule = idKotlinxSerializationModule()
        }
        val data = Data()
        val serialized = json.encodeToString(data)
        assertEquals(data, json.decodeFromString(serialized))
    }
}
