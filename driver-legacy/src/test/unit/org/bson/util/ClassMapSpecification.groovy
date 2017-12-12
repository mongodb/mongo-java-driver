/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package org.bson.util

import spock.lang.Specification

class ClassMapSpecification extends Specification {
    def 'should get the class instance when asking for a class'() {
        given:
        def classMap = new ClassMap<Animal>()
        def expected = new Animal()
        classMap.put(Animal, expected)

        when:
        def actual = classMap.get(Animal)

        then:
        actual == expected
    }

    def 'should return null if there is no matching class or superclass in the class map'() {
        given:
        def classMap = new ClassMap<Animal>()
        def expected = new Animal()
        classMap.put(Animal, expected)

        when:
        def actual = classMap.get(Object)

        then:
        actual == null
    }

    def 'should get the value of the most specific class'() {
        given:
        def classMap = new ClassMap<Animal>()
        def expected = new Dog()
        classMap.put(Animal, new Animal())
        classMap.put(Dog, expected)

        when:
        def actual = classMap.get(Dog)

        then:
        actual == expected
    }

    def 'should get the value of the superclass if specific class is not in the map'() {
        given:
        def classMap = new ClassMap<Animal>()
        def expected = new Animal()
        classMap.put(Animal, expected)
        classMap.put(Dog, new Dog())

        when:
        def actual = classMap.get(Duck)

        then:
        actual == expected
    }

    def 'should get the value of the closest superclass if specific class is not in the map'() {
        given:
        def classMap = new ClassMap<Animal>()
        def expected = new Dog()
        classMap.put(Animal, new Animal())
        classMap.put(Dog, expected)

        when:
        def actual = classMap.get(Labrador)

        then:
        actual == expected
    }

    def 'should get the hierarchy of all superclasses'() {
        when:
        def actual = ClassMap.getAncestry(Labrador)

        then:
        actual == [Labrador, Dog, Animal, GroovyObject, Object]
    }

    def 'should return the size for the number of class keys explicitly added to the map'() {
        // i.e. I don't expect it to add cached superclasses to the size
        given:
        def classMap = new ClassMap<Animal>()
        classMap.put(Animal, new Animal())
        classMap.put(Labrador, new Labrador())

        when:
        def actualSize = classMap.size()

        then:
        actualSize == 2
    }

    @SuppressWarnings('EmptyClass')
    private class Animal {

    }

    private class Dog extends Animal {

    }

    private class Labrador extends Dog {

    }

    private class Duck extends Animal {

    }
}
