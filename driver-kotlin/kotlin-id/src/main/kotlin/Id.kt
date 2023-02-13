/*
 *
 *  * Copyright 2008-present MongoDB, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.mongodb.kotlin.id

/**
 * A unique document identifier.
 *
 * If the id type need to support json serialization and deserialization,
 * it must provide a toString() method and a constructor with a one String arg,
 * and consistent equals & hashCode methods.
 *
 * Please note that equals and hashCode methods of Id are "implementation dependant":
 * if classes A and B are two implementations of Id, instances of A are always not equals to
 * instances of B.
 *
 * @param T the owner of the id
 */

interface Id<T> {

    /**
     * Cast Id<T> to Id<NewType>.
     */
    @Suppress("UNCHECKED_CAST")
    fun <NewType> cast(): Id<NewType> = this as Id<NewType>
}
