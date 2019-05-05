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

import spock.lang.Specification

import static java.util.Arrays.asList


class TagSetSpecification extends Specification {

    def 'should iterate an empty tag set'() {
        when:
        def tagSet = new TagSet()

        then:
        !tagSet.iterator().hasNext()
    }

    def 'should iterate a tag set with a single tag'() {
        def tag = new Tag('dc', 'ny')
        when:
        def tagSet = new TagSet(tag)
        def iterator = tagSet.iterator()
        then:
        iterator.hasNext()
        iterator.next() == tag
        !iterator.hasNext()
    }

    def 'should iterate a tag set with multiple tags'() {
        def tagOne = new Tag('dc', 'ny')
        def tagTwo = new Tag('rack', '1')
        when:
        def tagSet = new TagSet(asList(tagOne, tagTwo))
        def iterator = tagSet.iterator()
        then:
        iterator.hasNext()
        iterator.next() == tagOne
        iterator.hasNext()
        iterator.next() == tagTwo
        !iterator.hasNext()
    }

    def 'should throw on null argument'() {
        when:
        new TagSet((Tag) null)

        then:
        thrown(IllegalArgumentException)

        when:
        new TagSet((List<Tag>) null)

        then:
        thrown(IllegalArgumentException)

        when:
        new TagSet([new Tag('dc', 'ny'), null])

        then:
        thrown(IllegalArgumentException)
    }

    def 'should throw on duplicate tag name'() {
        when:
        new TagSet([new Tag('dc', 'ny'), new Tag('dc', 'ca')])

        then:
        thrown(IllegalArgumentException)
    }

    def 'should alphabetically order tags'() {
        when:
        def pTag = new Tag('p', '1')
        def dcTag = new Tag('dc', 'ny')
        def tagSet = new TagSet([pTag, dcTag])
        def iter = tagSet.iterator()

        then:
        iter.next() == dcTag
        iter.next() == pTag
        !iter.hasNext()
        tagSet == new TagSet([dcTag, pTag])
    }
}
