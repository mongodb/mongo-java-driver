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

package org.mongodb.kotlin.id.jackson

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.junit.Test
import org.mongodb.kotlin.id.Id
import org.mongodb.kotlin.id.jvm.toId
import kotlin.test.assertEquals

/**
 *
 */
class IdJsonTest {

    data class Data(
        val id: Id<Test> = "id".toId(),
        val set: Set<Id<Test>> = setOf(id),
        val map: Map<Id<Test>, Boolean> = mapOf(id to true)
    )

    @Test
    fun testSerializationAndDeserialization() {
        val data = Data()
        val mapper = jacksonObjectMapper().registerModule(IdJacksonModule())
        val json = mapper.writeValueAsString(data)
        assertEquals("{\"id\":\"id\",\"set\":[\"id\"],\"map\":{\"id\":true}}", json)
        assertEquals(data, mapper.readValue(json))
    }
}
