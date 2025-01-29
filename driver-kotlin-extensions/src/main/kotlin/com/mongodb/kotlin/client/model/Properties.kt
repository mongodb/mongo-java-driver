/*
 * Copyright 2008-present MongoDB, Inc.
 * Copyright (C) 2016/2022 Litote
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
 *
 * @custom-license-header
 */
package com.mongodb.kotlin.client.model

import com.mongodb.kotlin.client.property.KCollectionSimplePropertyPath
import com.mongodb.kotlin.client.property.KMapSimplePropertyPath
import com.mongodb.kotlin.client.property.KPropertyPath
import com.mongodb.kotlin.client.property.KPropertyPath.Companion.CustomProperty
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KProperty
import kotlin.reflect.KProperty1
import kotlin.reflect.full.findParameterByName
import kotlin.reflect.full.primaryConstructor
import kotlin.reflect.jvm.internal.ReflectProperties.lazySoft
import kotlin.reflect.jvm.javaField
import org.bson.codecs.pojo.annotations.BsonId
import org.bson.codecs.pojo.annotations.BsonProperty

private val pathCache: MutableMap<String, String> by lazySoft { ConcurrentHashMap<String, String>() }

/** Returns a composed property. For example Friend::address / Address::postalCode = "address.postalCode". */
public operator fun <T0, T1, T2> KProperty1<T0, T1?>.div(p2: KProperty1<T1, T2?>): KProperty1<T0, T2?> =
    KPropertyPath(this, p2)

/**
 * Returns a composed property without type checks. For example Friend::address % Address::postalCode =
 * "address.postalCode".
 */
public operator fun <T0, T1, T2> KProperty1<T0, T1?>.rem(p2: KProperty1<out T1, T2?>): KProperty1<T0, T2?> =
    KPropertyPath(this, p2)

/**
 * Returns a collection composed property. For example Friend::addresses / Address::postalCode = "addresses.postalCode".
 */
@JvmName("divCol")
public operator fun <T0, T1, T2> KProperty1<T0, Iterable<T1>?>.div(p2: KProperty1<out T1, T2?>): KProperty1<T0, T2?> =
    KPropertyPath(this, p2)

/** Returns a map composed property. For example Friend::addresses / Address::postalCode = "addresses.postalCode". */
@JvmName("divMap")
public operator fun <T0, K, T1, T2> KProperty1<T0, Map<out K, T1>?>.div(
    p2: KProperty1<out T1, T2?>
): KProperty1<T0, T2?> = KPropertyPath(this, p2)

/**
 * Returns a mongo path of a property.
 *
 * The path name is computed by checking the following and picking the first value to exist:
 * - SerialName annotation value
 * - BsonId annotation use '_id'
 * - BsonProperty annotation
 * - Property name
 */
public fun <T> KProperty<T>.path(): String {
    return if (this is KPropertyPath<*, T>) {
        this.name
    } else {
        pathCache.computeIfAbsent(this.toString()) {

            // Check serial name - Note kotlinx.serialization.SerialName may not be on the class
            // path
            val serialName =
                annotations.firstOrNull { it.annotationClass.qualifiedName == "kotlinx.serialization.SerialName" }
            var path =
                serialName?.annotationClass?.members?.firstOrNull { it.name == "value" }?.call(serialName) as String?

            // If no path (serialName) then check for BsonId / BsonProperty
            if (path == null) {
                val originator = if (this is CustomProperty<*, *>) this.previous.property else this
                // If this property is calculated (doesn't have a backing field) ex
                // "(Student::grades / Grades::score).posOp then
                // originator.javaField will NPE.
                // Only read various annotations on a declared property with a backing field
                if (originator.javaField != null) {
                    val constructorProperty =
                        originator.javaField!!.declaringClass.kotlin.primaryConstructor?.findParameterByName(this.name)

                    // Prefer BsonId annotation over BsonProperty
                    path = constructorProperty?.annotations?.filterIsInstance<BsonId>()?.firstOrNull()?.let { "_id" }
                    path =
                        path ?: constructorProperty?.annotations?.filterIsInstance<BsonProperty>()?.firstOrNull()?.value
                }
                path = path ?: this.name
            }
            path
        }
    }
}

/** Returns a collection property. */
public val <T> KProperty1<out Any?, Iterable<T>?>.colProperty: KCollectionSimplePropertyPath<out Any?, T>
    get() = KCollectionSimplePropertyPath(null, this)

/** In order to write array indexed expressions (like `accesses.0.timestamp`). */
public fun <T> KProperty1<out Any?, Iterable<T>?>.pos(position: Int): KPropertyPath<out Any?, T?> =
    colProperty.pos(position)

/** Returns a map property. */
public val <K, T> KProperty1<out Any?, Map<out K, T>?>.mapProperty: KMapSimplePropertyPath<out Any?, K, T>
    get() = KMapSimplePropertyPath(null, this)

@Suppress("MaxLineLength")
/**
 * [The positional array operator $ (projection or update)](https://docs.mongodb.com/manual/reference/operator/update/positional/)
 */
public val <T> KProperty1<out Any?, Iterable<T>?>.posOp: KPropertyPath<out Any?, T?>
    get() = colProperty.posOp

@Suppress("MaxLineLength")
/** [The all positional operator $[]](https://docs.mongodb.com/manual/reference/operator/update/positional-all/) */
public val <T> KProperty1<out Any?, Iterable<T>?>.allPosOp: KPropertyPath<out Any?, T?>
    get() = colProperty.allPosOp

@Suppress("MaxLineLength")
/**
 * [The filtered positional operator $[\<identifier\>]](https://docs.mongodb.com/manual/reference/operator/update/positional-filtered/)
 */
public fun <T> KProperty1<out Any?, Iterable<T>?>.filteredPosOp(identifier: String): KPropertyPath<out Any?, T?> =
    colProperty.filteredPosOp(identifier)

/** Key projection of map. Sample: `p.keyProjection(Locale.ENGLISH) / Gift::amount` */
@Suppress("UNCHECKED_CAST")
public fun <K, T> KProperty1<out Any?, Map<out K, T>?>.keyProjection(key: K): KPropertyPath<Any?, T?> =
    mapProperty.keyProjection(key) as KPropertyPath<Any?, T?>
