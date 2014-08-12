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

}