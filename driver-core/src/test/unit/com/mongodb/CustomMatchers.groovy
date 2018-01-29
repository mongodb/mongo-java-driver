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

package com.mongodb

import org.hamcrest.BaseMatcher
import org.hamcrest.Description

import java.lang.reflect.Field
import java.lang.reflect.Modifier

@SuppressWarnings('NoDef')
class CustomMatchers {

    static nullList = [null, null]
    static isTheSameAs(final Object e) {
        [
                matches         : { a -> compare(e, a) },
                describeTo      : { Description description -> description.appendText("Operation has the same attributes ${e.class.name}")
                },
                describeMismatch: { a, description -> describer(e, a, description) }
        ] as BaseMatcher
    }

    static isTheSameAs(final Object e, final List<String> ignoreNames) {
        [
                matches         : { a -> compare(e, a, ignoreNames) },
                describeTo      : { Description description -> description.appendText("Operation has the same attributes ${e.class.name}")
                },
                describeMismatch: { a, description -> describer(e, a, ignoreNames, description) }
        ] as BaseMatcher
    }

    static compare(expected, actual) {
        compare(expected, actual, [])
    }

    static compare(expected, actual, ignoreNames) {
        if (expected == actual) {
            return true
        }
        if (expected == null || actual == null) {
            return false
        }
        if (actual.class.name != expected.class.name) {
            return false
        }
        getFields(actual.class).findAll { !ignoreNames.contains(it.name) } .collect {
            it.setAccessible(true)
            def actualPropertyValue = it.get(actual)
            def expectedPropertyValue = it.get(expected)

            if (nominallyTheSame(it.name)) {
                return actualPropertyValue.class == expectedPropertyValue.class
            } else if (actualPropertyValue != expectedPropertyValue) {
                if ([actualPropertyValue, expectedPropertyValue].contains(null)
                        && [actualPropertyValue, expectedPropertyValue] != nullList) {
                    return false
                } else if (List.isCase(actualPropertyValue) && List.isCase(expectedPropertyValue)
                        && (actualPropertyValue.size() == expectedPropertyValue.size())) {
                    def i = -1
                    return actualPropertyValue.collect { a -> i++; compare(a, expectedPropertyValue[i]) }.every { it }
                } else if (actualPropertyValue.class != null && actualPropertyValue.class.name.startsWith('com.mongodb')
                        && actualPropertyValue.class == expectedPropertyValue.class) {
                    return compare(actualPropertyValue, expectedPropertyValue)
                }
                return false
            }
            true
        }.every { it }
    }


    static describer(expected, actual, description) {
        describer(expected, actual, [], description)
    }

    static describer(expected, actual, ignoreNames, description) {
        if (expected == actual) {
            return true
        }
        if (expected == null || actual == null) {
            description.appendText("different values: $expected != $actual, ")
            return false
        }
        if (actual.class.name != expected.class.name) {
            description.appendText("different classes: ${expected.class.name} != ${actual.class.name}, ")
            return false
        }

        getFields(actual.class).findAll { !ignoreNames.contains(it.name) } .collect {
            it.setAccessible(true)
            def actualPropertyValue = it.get(actual)
            def expectedPropertyValue = it.get(expected)
            if (nominallyTheSame(it)) {
                if (actualPropertyValue.class != expectedPropertyValue.class) {
                    description.appendText("different classes in $it.name :" +
                            " ${expectedPropertyValue.class.name} != ${actualPropertyValue.class.name}, ")
                    return false
                }
            } else if (actualPropertyValue != expectedPropertyValue) {
                if (([actualPropertyValue, expectedPropertyValue].contains(null)
                        || [actualPropertyValue.class, expectedPropertyValue.class].contains(null))
                        && [actualPropertyValue, expectedPropertyValue] != nullList) {
                    description.appendText("different values in $it.name : ${expectedPropertyValue} != ${actualPropertyValue}\n")
                    return false
                } else if (List.isCase(actualPropertyValue) && List.isCase(expectedPropertyValue)
                        && (actualPropertyValue.size() == expectedPropertyValue.size())) {
                    def i = -1
                    actualPropertyValue.each { a ->
                        i++; if (!compare(a, expectedPropertyValue[i])) {
                            describer(a, expectedPropertyValue[i], description)
                        }
                    }.every { it }
                } else if (actualPropertyValue.class.name.startsWith('com.mongodb')
                        && actualPropertyValue.class == expectedPropertyValue.class) {
                    return describer(actualPropertyValue, expectedPropertyValue, description)
                }
                description.appendText("different values in $it.name : ${expectedPropertyValue} != ${actualPropertyValue}\n")
                return false
            }
            true
        }
    }

    static List<Field> getFields(Class curClass) {
        if (curClass == Object) {
            return []
        }
        def fields = getFields(curClass.getSuperclass())
        fields.addAll(curClass.declaredFields.findAll { !it.synthetic && !Modifier.isStatic(it.modifiers) })
        fields
    }


    static nominallyTheSame(String propertyName ) {
        propertyName in ['decoder', 'executor']
    }
}
