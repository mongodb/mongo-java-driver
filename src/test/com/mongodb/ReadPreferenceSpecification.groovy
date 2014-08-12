package com.mongodb

import spock.lang.Specification

@SuppressWarnings(['DuplicateMapLiteral', 'LineLength'])
class ReadPreferenceSpecification extends Specification {

    def 'should have correct names'() {
        expect:
        readPreference.getName() == name

        where:
        readPreference                      | name
        ReadPreference.primary()            | 'primary'
        ReadPreference.primaryPreferred()   | 'primaryPreferred'
        ReadPreference.secondary()          | 'secondary'
        ReadPreference.secondaryPreferred() | 'secondaryPreferred'
        ReadPreference.nearest()            | 'nearest'
    }

    def 'should have correct valueOf'() {
        expect:
        ReadPreference.primary() == ReadPreference.valueOf('primary')
        ReadPreference.secondary() == ReadPreference.valueOf('secondary')
        ReadPreference.primaryPreferred() == ReadPreference.valueOf('primaryPreferred')
        ReadPreference.secondaryPreferred() == ReadPreference.valueOf('secondaryPreferred')
        ReadPreference.nearest() == ReadPreference.valueOf('nearest')
    }

    def 'valueOf should throw with null name'() {
        when:
        ReadPreference.valueOf(null)

        then:
        thrown(IllegalArgumentException)

        when:
        ReadPreference.valueOf(null, new BasicDBObject('dc', 'ny'))

        then:
        thrown(IllegalArgumentException)
    }

    def 'valueOf should throw with unexpected name'() {
        when:
        ReadPreference.valueOf('unknown')

        then:
        thrown(IllegalArgumentException)

        when:
        ReadPreference.valueOf(null, new BasicDBObject('dc', 'ny'))

        then:
        thrown(IllegalArgumentException)
    }

    def 'should have correct valueOf with tag set list'() {
        def tags = [new TagSet([new Tag('dy', 'ny'), new Tag('rack', '1')]), new TagSet([new Tag('dy', 'ca'), new Tag('rack', '2')])]
        def firstTagSetDocument = new BasicDBObject('dy', 'ny').append('rack', '1')
        def secondTagSetDocument = new BasicDBObject('dy', 'ca').append('rack', '2')

        expect:
        ReadPreference.secondary(tags) == ReadPreference.valueOf('secondary', tags)
        ReadPreference.primaryPreferred(tags) == ReadPreference.valueOf('primaryPreferred', tags)
        ReadPreference.secondaryPreferred(tags) == ReadPreference.valueOf('secondaryPreferred', tags)
        ReadPreference.nearest(tags) == ReadPreference.valueOf('nearest', tags)

        ReadPreference.secondary(tags) == ReadPreference.valueOf('secondary', firstTagSetDocument, secondTagSetDocument)
        ReadPreference.primaryPreferred(tags) == ReadPreference.valueOf('primaryPreferred', firstTagSetDocument, secondTagSetDocument)
        ReadPreference.secondaryPreferred(tags) == ReadPreference.valueOf('secondaryPreferred', firstTagSetDocument, secondTagSetDocument)
        ReadPreference.nearest(tags) == ReadPreference.valueOf('nearest', firstTagSetDocument, secondTagSetDocument)
    }

    def 'should have correct valueOf with tag set documents'() {
        def tags = [new TagSet([new Tag('dy', 'ny'), new Tag('rack', '1')]), new TagSet([new Tag('dy', 'ca'), new Tag('rack', '2')])]
        expect:
        ReadPreference.secondary(tags) == ReadPreference.valueOf('secondary', tags)
        ReadPreference.primaryPreferred(tags) == ReadPreference.valueOf('primaryPreferred', tags)
        ReadPreference.secondaryPreferred(tags) == ReadPreference.valueOf('secondaryPreferred', tags)
        ReadPreference.nearest(tags) == ReadPreference.valueOf('nearest', tags)
    }

    def 'should convert simple read preferences to correct documents'() {
        expect:
        readPreference.toDBObject() == document

        where:
        readPreference                      | document
        ReadPreference.primary()            | new BasicDBObject('mode', 'primary')
        ReadPreference.primaryPreferred()   | new BasicDBObject('mode', 'primaryPreferred')
        ReadPreference.secondary()          | new BasicDBObject('mode', 'secondary')
        ReadPreference.secondaryPreferred() | new BasicDBObject('mode', 'secondaryPreferred')
        ReadPreference.nearest()            | new BasicDBObject('mode', 'nearest')
    }

    def 'should convert read preferences with a single tag set to correct documents'() {
        expect:
        readPreference.toDBObject() == document

        where:
        readPreference                                                   | document
        ReadPreference.primaryPreferred(
                new TagSet([new Tag('dc', 'ny'), new Tag('rack', '1')])) | new BasicDBObject('mode', 'primaryPreferred')
                .append('tags', [new BasicDBObject('dc', 'ny').append('rack', '1')])

        ReadPreference.secondary(
                new TagSet([new Tag('dc', 'ny'), new Tag('rack', '1')])) | new BasicDBObject('mode', 'secondary')
                .append('tags', [new BasicDBObject('dc', 'ny').append('rack', '1')])

        ReadPreference.secondaryPreferred(
                new TagSet([new Tag('dc', 'ny'), new Tag('rack', '1')])) | new BasicDBObject('mode', 'secondaryPreferred')
                .append('tags', [new BasicDBObject('dc', 'ny').append('rack', '1')])

        ReadPreference.nearest(
                new TagSet([new Tag('dc', 'ny'), new Tag('rack', '1')])) | new BasicDBObject('mode', 'nearest')
                .append('tags', [new BasicDBObject('dc', 'ny').append('rack', '1')])

    }

    def 'should convert read preferences with a tag set list to correct documents'() {
        expect:
        readPreference.toDBObject() == document

        where:
        readPreference                                                     | document
        ReadPreference.primaryPreferred(
                [new TagSet([new Tag('dc', 'ny'), new Tag('rack', '1')])]) | new BasicDBObject('mode', 'primaryPreferred')
                .append('tags', [new BasicDBObject('dc', 'ny').append('rack', '1')])

        ReadPreference.secondary(
                [new TagSet([new Tag('dc', 'ny'), new Tag('rack', '1')])]) | new BasicDBObject('mode', 'secondary')
                .append('tags', [new BasicDBObject('dc', 'ny').append('rack', '1')])

        ReadPreference.secondaryPreferred(
                [new TagSet([new Tag('dc', 'ny'), new Tag('rack', '1')])]) | new BasicDBObject('mode', 'secondaryPreferred')
                .append('tags', [new BasicDBObject('dc', 'ny').append('rack', '1')])

        ReadPreference.nearest(
                [new TagSet([new Tag('dc', 'ny'), new Tag('rack', '1')])]) | new BasicDBObject('mode', 'nearest')
                .append('tags', [new BasicDBObject('dc', 'ny').append('rack', '1')])

        ReadPreference.nearest(
                [new TagSet([new Tag('dc', 'ny'), new Tag('rack', '1')]),
                 new TagSet([new Tag('dc', 'ca'), new Tag('rack', '2')])]) | new BasicDBObject('mode', 'nearest')
                .append('tags', [new BasicDBObject('dc', 'ny').append('rack', '1'),
                                 new BasicDBObject('dc', 'ca').append('rack', '2')])
    }

    // testing deprecated methods
    def 'should convert read preferences with a list of tag set documents to correct documents'() {
        expect:
        readPreference.toDBObject() == document

        where:
        readPreference                                                     | document
        ReadPreference.primaryPreferred(
                new BasicDBObject('dc', 'ny').append('rack', '1'))         | new BasicDBObject('mode', 'primaryPreferred')
                .append('tags', [new BasicDBObject('dc', 'ny').append('rack', '1')])

        ReadPreference.secondary(
                new BasicDBObject('dc', 'ny').append('rack', '1'))         | new BasicDBObject('mode', 'secondary')
                .append('tags', [new BasicDBObject('dc', 'ny').append('rack', '1')])

        ReadPreference.secondaryPreferred(
                new BasicDBObject('dc', 'ny').append('rack', '1'))         | new BasicDBObject('mode', 'secondaryPreferred')
                .append('tags', [new BasicDBObject('dc', 'ny').append('rack', '1')])

        ReadPreference.nearest(
                new BasicDBObject('dc', 'ny').append('rack', '1'))         | new BasicDBObject('mode', 'nearest')
                .append('tags', [new BasicDBObject('dc', 'ny').append('rack', '1')])

        ReadPreference.nearest(
                new BasicDBObject('dc', 'ny').append('rack', '1'),
                new BasicDBObject('dc', 'ca').append('rack', '2'),
                new BasicDBObject('dc', 'ca').append('rack', '3'))         | new BasicDBObject('mode', 'nearest')
                .append('tags', [new BasicDBObject('dc', 'ny').append('rack', '1'),
                                 new BasicDBObject('dc', 'ca').append('rack', '2'),
                                 new BasicDBObject('dc', 'ca').append('rack', '3')])
    }
}
